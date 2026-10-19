package seeyon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
)

// The Seeyon MCP gateway is a stateless streamable-HTTP JSON-RPC 2.0 endpoint:
//
//   - there is no Mcp-Session-Id and no required handshake — tools/list and
//     tools/call answer without a prior initialize;
//   - every response is HTTP 200, including failures, so the JSON-RPC "error"
//     object (and result.isError for tool-level refusals) is the only signal;
//   - protocolVersion is pinned server-side to 2024-11-05;
//   - the body is plain JSON when Accept excludes text/event-stream, and an SSE
//     "event:message\ndata:{...}" frame otherwise.
//
// This is deliberately a small dedicated client rather than a reuse of
// internal/mcp: that package carries protocol negotiation, session management
// and 401→OAuth discovery, none of which apply to a stateless gateway, and its
// configuration is bound to the MCP *service* model rather than to a data
// source's own credentials. Every other connector in this package follows the
// same "own thin client on datasource.NewConnectorHTTPClient" pattern.

// requestTimeout bounds a single gateway round trip. The report runtime pages
// through CAP4 and can be slow on a large report, so this is generous compared
// with a plain REST call.
const requestTimeout = 60 * time.Second

// Package-level sentinel errors so callers (and tests) can distinguish the
// three failure modes an operator must act on differently.
var (
	// errAuthorizationRejected means the gateway refused the configured MCP
	// personal authorization code (wrong, expired, disabled or reset).
	errAuthorizationRejected = errors.New("seeyon mcp gateway rejected the authorization code")
	// errToolNotAuthorized means the gateway knows the tool but the
	// authorization code's scope does not cover it. This is a gateway-side
	// configuration problem, not a credential problem.
	errToolNotAuthorized = errors.New("seeyon mcp gateway has not authorized this tool")
	// errToolNotFound means the connector could not resolve a required report
	// tool from tools/list.
	errToolNotFound = errors.New("seeyon report tool not found on the gateway")
)

// mcpTool is one entry of the gateway's tools/list result.
type mcpTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema,omitempty"`
}

type rpcRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

type contentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type callToolResult struct {
	Content           []contentItem   `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError,omitempty"`
}

// client is a minimal Seeyon MCP gateway client.
//
// It is NOT safe for concurrent use: resolveTools writes the resolved tool map
// and the legacy-tool marker, and call increments a request counter. A single
// connect-and-fetch run in this connector is sequential, which is what the
// gateway's own rate characteristics suit anyway.
type client struct {
	baseURL    string
	accessCode string
	http       *http.Client
	// tools maps a tool role to the concrete gateway tool name.
	tools map[string]string
	// available is the last tools/list payload, kept so a caller can fall back
	// to the runner-up when the gateway rejects its first choice.
	available []mcpTool
	// legacyTool is the pre-report-runtime single report tool, when the
	// deployment still exposes one. It dispatches on designType, unlike the
	// report-runtime tools, so the caller must send that field only for it.
	legacyTool string
	// nextID is only used for request correlation in logs; the gateway ignores
	// it, so a per-client counter is enough and avoids an atomic.
	nextID int
}

func newClient(baseURL, accessToken string) (*client, error) {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return nil, fmt.Errorf(
			"%w: credentials.base_url is required", datasource.ErrInvalidConfig,
		)
	}
	// Validate the operator-supplied endpoint against the deployment's SSRF
	// policy before any request is attempted, so a blocked private address
	// fails with the policy's own actionable message instead of a dial timeout.
	if err := datasource.ValidateConnectorBaseURL(trimmed); err != nil {
		return nil, fmt.Errorf("%w: %v", datasource.ErrInvalidConfig, err)
	}
	token := strings.TrimSpace(accessToken)
	if token == "" {
		return nil, fmt.Errorf(
			"%w: credentials.access_token is required",
			datasource.ErrInvalidConfig,
		)
	}
	return &client{
		baseURL:    trimmed,
		accessCode: token,
		http:       datasource.NewConnectorHTTPClient(requestTimeout),
	}, nil
}

// call performs one JSON-RPC request and returns the raw "result" object.
//
// Every gateway response is HTTP 200, so a non-200 status is treated as a
// transport/proxy problem and reported with the status line; a JSON-RPC "error"
// object is translated into one of the package sentinels.
func (c *client) call(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	c.nextID++
	body, err := json.Marshal(rpcRequest{
		JSONRPC: "2.0",
		ID:      c.nextID,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", method, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build %s request: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Offering both is what the product's own client does; the gateway then
	// answers with SSE, which parseBody handles alongside plain JSON.
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Authorization", "Bearer "+c.accessCode)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, c.baseURL, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf(
			"%s %s: unexpected HTTP %d: %s",
			method, c.baseURL, resp.StatusCode, truncateForError(raw),
		)
	}

	payload, err := extractJSONPayload(raw, resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, c.baseURL, err)
	}

	var decoded rpcResponse
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, fmt.Errorf("%s %s: decode JSON-RPC response: %w", method, c.baseURL, err)
	}
	if decoded.Error != nil {
		return nil, classifyRPCError(method, decoded.Error)
	}
	if len(decoded.Result) == 0 {
		return nil, fmt.Errorf("%s %s: JSON-RPC response carried no result", method, c.baseURL)
	}
	return decoded.Result, nil
}

// errNeedsConfirmation marks the gateway's "this tool needs a two-step
// confirmation ticket" rejection. The gateway application classifies
// the report tools as 中危/高危 and refuses a call without a _confirmUuid.
var errNeedsConfirmation = errors.New("seeyon tool needs a confirmation ticket")

// confirmToolName is the gateway's own ticket-issuing tool. It is a fixed,
// human-readable name (unlike the opaque report tool ids), so it is safe to
// reference directly: the gateway advertises it as "confirm_tool_call".
const confirmToolName = "confirm_tool_call"

// maxResponseBytes bounds a single response body. A 500-row page of a wide
// report is well under this; the limit exists so a hostile or broken endpoint
// cannot exhaust memory.
const maxResponseBytes = 64 << 20 // 64 MiB

// classifyRPCError turns a JSON-RPC error into an actionable Go error, keeping
// the gateway's own text (which is already user-facing Chinese) as context.
func classifyRPCError(method string, rpcErr *rpcError) error {
	message := strings.TrimSpace(rpcErr.Message)
	lowered := strings.ToLower(message)
	switch {
	case strings.Contains(message, "二次确认"), strings.Contains(lowered, "confirm"):
		return fmt.Errorf("%w: %s", errNeedsConfirmation, message)
	case strings.Contains(message, "授权码"), strings.Contains(message, "授权码错误"),
		strings.Contains(lowered, "authorization"):
		return fmt.Errorf(
			"%w: %s — renew or re-copy the code from V5 个人设置 → 任务令牌 "+
				"(the code expires after 30 days and has no refresh flow)",
			errAuthorizationRejected, message,
		)
	default:
		return fmt.Errorf("%s: gateway error %d: %s", method, rpcErr.Code, message)
	}
}

// extractJSONPayload returns the JSON document carried by a gateway response,
// accepting either a plain JSON body or an SSE frame. The gateway answers with
// SSE whenever text/event-stream is offered, so both must work.
func extractJSONPayload(raw []byte, contentType string) ([]byte, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("empty response body")
	}
	if trimmed[0] == '{' {
		return trimmed, nil
	}
	// SSE: take the last "data:" line, which is where the JSON-RPC frame lands.
	var payload []byte
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if value != "" {
			payload = []byte(value)
		}
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf(
			"response was neither JSON nor an SSE data frame (content-type %q): %s",
			contentType, truncateForError(raw),
		)
	}
	return payload, nil
}

// truncateForError keeps an unexpected body short enough for a log line.
func truncateForError(raw []byte) string {
	const limit = 512
	text := strings.TrimSpace(string(raw))
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "…"
}

// initialize proves the endpoint is reachable and the authorization code is
// accepted. The gateway is stateless, so this is a probe rather than a
// handshake: nothing from the response is retained.
func (c *client) initialize(ctx context.Context) error {
	result, err := c.call(ctx, "initialize", map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]interface{}{},
		"clientInfo":      map[string]interface{}{"name": "weknora-seeyon-datasource", "version": "1.0"},
	})
	if err != nil {
		return err
	}
	var info struct {
		ServerInfo struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
	}
	if err := json.Unmarshal(result, &info); err != nil {
		// A reachable gateway that answers initialize with an unexpected shape
		// is still proof the credential works; do not fail the connection test
		// over a field we do not use.
		logger.Warnf(ctx, "[Seeyon] initialize returned an unexpected result shape: %v", err)
	}
	return nil
}

// listTools returns every tool the authorization code may use. The listing is
// itself scoped by the gateway's authorization, so a tool that exists in the OA
// build but is not granted to this app never appears here.
func (c *client) listTools(ctx context.Context) ([]mcpTool, error) {
	result, err := c.call(ctx, "tools/list", map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	var decoded struct {
		Tools []mcpTool `json:"tools"`
	}
	if err := json.Unmarshal(result, &decoded); err != nil {
		return nil, fmt.Errorf("decode tools/list result: %w", err)
	}
	return decoded.Tools, nil
}

// callTool runs a tool and returns its payload as JSON.
//
// Tool results arrive as a JSON *string* inside content[0].text (the gateway
// also mirrors the same object into structuredContent), so the payload is
// extracted and re-parsed before the caller sees it.
func (c *client) callTool(ctx context.Context, name string, args map[string]interface{}) (json.RawMessage, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: empty tool name", errToolNotFound)
	}
	if args == nil {
		args = map[string]interface{}{}
	}
	result, err := c.callToolOnce(ctx, name, args)
	if errors.Is(err, errNeedsConfirmation) && name != confirmToolName {
		// The ticket is single-use, so every rejected call gets its own. Retrying
		// once is safe for the tools this connector uses: they are all reads, and
		// the rejection happened before the gateway did any work.
		uuid, ticketErr := c.requestConfirmation(ctx, name)
		if ticketErr != nil {
			return nil, ticketErr
		}
		args["_confirmUuid"] = uuid
		result, err = c.callToolOnce(ctx, name, args)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	var call callToolResult
	if err := json.Unmarshal(result, &call); err != nil {
		return nil, fmt.Errorf("decode tools/call result for %s: %w", name, err)
	}

	text := firstTextContent(call.Content)
	if call.IsError {
		if strings.Contains(text, "未被允许调用") || strings.Contains(text, "not allowed") {
			return nil, fmt.Errorf("%w: %s", errToolNotAuthorized, text)
		}
		return nil, fmt.Errorf("seeyon tool %s failed: %s", name, text)
	}

	payload := payloadFrom(call, text)
	if len(payload) == 0 {
		return nil, fmt.Errorf("seeyon tool %s returned an empty payload", name)
	}
	return payload, nil
}

// callToolOnce performs exactly one tools/call round trip.
func (c *client) callToolOnce(
	ctx context.Context, name string, args map[string]interface{},
) (json.RawMessage, error) {
	return c.call(ctx, "tools/call", map[string]interface{}{
		"name":      name,
		"arguments": args,
	})
}

// requestConfirmation exchanges a tool name for a single-use confirmation ticket.
//
// The gateway returns {"uuid":"…","toolName":"…","expiresAt":<epochMillis>}; the
// ticket is bound to the tool and must be replayed as _confirmUuid on the call
// it was issued for.
func (c *client) requestConfirmation(ctx context.Context, toolName string) (string, error) {
	result, err := c.call(ctx, "tools/call", map[string]interface{}{
		"name":      confirmToolName,
		"arguments": map[string]interface{}{"toolName": toolName},
	})
	if err != nil {
		return "", err
	}
	var call callToolResult
	if err := json.Unmarshal(result, &call); err != nil {
		return "", fmt.Errorf("decode %s result: %w", confirmToolName, err)
	}
	text := firstTextContent(call.Content)
	if call.IsError {
		return "", fmt.Errorf(
			"%w: the gateway refused to issue a confirmation ticket for %s: %s",
			errToolNotFound, toolName, text,
		)
	}
	payload := payloadFrom(call, text)
	// The ticket is wrapped in the same {code,message,data} envelope as every
	// other tool result.
	inner, err := unwrap(confirmToolName, payload)
	if err != nil {
		return "", err
	}
	var ticket struct {
		UUID string `json:"uuid"`
	}
	if err := json.Unmarshal(inner, &ticket); err != nil {
		return "", fmt.Errorf("decode confirmation ticket: %w", err)
	}
	ticket.UUID = strings.TrimSpace(ticket.UUID)
	if ticket.UUID == "" {
		return "", fmt.Errorf(
			"the gateway issued an empty confirmation ticket for %s", toolName,
		)
	}
	return ticket.UUID, nil
}

// firstTextContent returns the first text item of a tool result.
func firstTextContent(items []contentItem) string {
	for _, item := range items {
		if item.Type == "" || item.Type == "text" {
			if strings.TrimSpace(item.Text) != "" {
				return item.Text
			}
		}
	}
	return ""
}

// payloadFrom prefers structuredContent, falling back to parsing the text item.
// Both carry the OA envelope {"code":…,"message":…,"data":…}.
func payloadFrom(call callToolResult, text string) json.RawMessage {
	structured := bytes.TrimSpace(call.StructuredContent)
	if len(structured) > 0 && !bytes.Equal(structured, []byte("null")) {
		if json.Valid(structured) {
			return structured
		}
	}
	trimmed := strings.TrimSpace(text)
	if trimmed == "" || !json.Valid([]byte(trimmed)) {
		return nil
	}
	return json.RawMessage(trimmed)
}

// resolveTools maps each tool role to a concrete gateway tool name.
//
// Tool names on this gateway are opaque (32-hex ids, or "sy-"/"cs-" + MD5) and
// are regenerated when the OA build's API set changes, so they are matched by
// their human-readable description and can always be pinned explicitly through
// settings.tool_overrides.
func (c *client) resolveTools(ctx context.Context, overrides map[string]string) error {
	tools, err := c.listTools(ctx)
	if err != nil {
		return err
	}
	resolved, err := c.resolveToolsFrom(ctx, tools, overrides)
	if err != nil {
		return err
	}
	c.tools = resolved
	return nil
}

// rankedTools returns the role's candidates, best first. An explicit override
// wins outright: the operator pinned it, so the fallback must not second-guess.
func (c *client) rankedTools(role string, limit int) []string {
	if pinned := strings.TrimSpace(c.tools[role]); pinned != "" && c.available == nil {
		return []string{pinned}
	}
	ranked := rankTools(c.available, role, limit)
	out := make([]string, 0, len(ranked)+1)
	seen := make(map[string]bool, len(ranked)+1)
	if pinned := strings.TrimSpace(c.tools[role]); pinned != "" {
		out = append(out, pinned)
		seen[pinned] = true
	}
	for _, candidate := range ranked {
		if seen[candidate.name] {
			continue
		}
		out = append(out, candidate.name)
	}
	return out
}

// resolveToolsFrom is the pure half of resolveTools, split out so the matching
// rules can be tested without an HTTP server.
func (c *client) resolveToolsFrom(
	ctx context.Context, tools []mcpTool, overrides map[string]string,
) (map[string]string, error) {
	c.available = tools
	resolved := make(map[string]string, len(toolRoles))
	known := make(map[string]bool, len(tools))
	for _, tool := range tools {
		known[tool.Name] = true
	}

	for _, role := range toolRoles {
		if override := strings.TrimSpace(overrides[role]); override != "" {
			if !known[override] {
				logger.Warnf(ctx,
					"[Seeyon] settings.tool_overrides.%s=%q is not in tools/list; "+
						"using it anyway (the gateway may resolve it server-side)", role, override)
			}
			resolved[role] = override
			continue
		}
		ranked := rankTools(tools, role, 3)
		if len(ranked) == 0 {
			continue
		}
		name, score := ranked[0].name, ranked[0].score
		// Log the runners-up too: when the gateway rewords its descriptions the
		// wrong tool can win, and this line is the only way to see why without
		// re-running against a token.
		alternatives := make([]string, 0, len(ranked)-1)
		for _, candidate := range ranked[1:] {
			alternatives = append(alternatives, fmt.Sprintf("%s=%d", candidate.name, candidate.score))
		}
		logger.Infof(ctx, "[Seeyon] resolved tool role %s -> %s (score %d; next: %v)",
			role, name, score, alternatives)
		resolved[role] = name
	}

	// Legacy fallback: an OA build that predates the report-runtime split
	// exposes a single "获取报表具体数据" tool that dispatches on designType
	// itself. Reusing it for both roles keeps such a deployment working.
	if resolved[toolRoleQuery] == "" || resolved[toolRoleStatistics] == "" {
		if legacy := legacyReportTool(tools); legacy != "" {
			c.legacyTool = legacy
			for _, role := range []string{toolRoleQuery, toolRoleStatistics} {
				if resolved[role] == "" {
					logger.Infof(ctx, "[Seeyon] falling back to legacy report tool %s for role %s", legacy, role)
					resolved[role] = legacy
				}
			}
		}
	}

	var missing []string
	for _, role := range toolRoles {
		if resolved[role] == "" {
			missing = append(missing, role)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"%w: could not resolve %s among the %d tools returned by tools/list; "+
				"the gateway app is usually not authorized for the 报表管理 tools — "+
				"grant that tool group, or pin the names via settings.tool_overrides",
			errToolNotFound, strings.Join(missing, ", "), len(tools),
		)
	}
	return resolved, nil
}

// Description phrases the connector matches on. They are the @Operation
// summaries comi-mcp ships, plus the legacy single-tool summary.
const (
	phraseReportsNew    = "获取有权限的报表列表"
	phraseReportsLegacy = "获取用户有权限的报表列表"
	phraseQueryNew      = "搜索查询报表数据"
	phraseQueryShort    = "查询报表数据"
	phraseStatsNew      = "搜索统计报表数据"
	phraseStatsShort    = "统计报表数据"
	phraseLegacyReport  = "获取报表具体数据"
)

// Tool descriptions on this gateway are not a bare summary: comi-mcp ships the
// operation summary followed by prose that names the *other* report tools
// ("先调用获取有权限的报表列表拿到报表ID…"). A plain substring match therefore
// scores the wrong tool at full marks — in production the listing role resolved
// to the query data tool and every listing call failed with
// 报表ID和报表名称至少传入一个.
//
// Two independent signals fix that:
//
//   - Position: a phrase only counts fully when the description *opens* with it.
//     A mention further in the text is worth a fraction, so it can never outrank
//     the tool whose summary it is.
//   - Schema: only the listing tool declares designType, and only the data tools
//     page rows. This is wording-free, so a rewording cannot break it.
const (
	scorePhraseLeading  = 200
	scorePhraseNearHead = 120
	scorePhraseMention  = 60
	scorePhraseFallback = 20
	headWindowBytes     = 60
)

// bestToolFor scores every tool against one role and returns the best match.
// The highest score wins; ties are broken by tool name so the choice is stable
// across runs (map iteration order is not).
func bestToolFor(tools []mcpTool, role string) (string, int) {
	ranked := rankTools(tools, role, 1)
	if len(ranked) == 0 {
		return "", 0
	}
	return ranked[0].name, ranked[0].score
}

type toolScore struct {
	name  string
	score int
}

// rankTools orders the tools that match a role, best first. Callers use more
// than one entry to fall back when the gateway rejects the top choice.
func rankTools(tools []mcpTool, role string, limit int) []toolScore {
	scored := make([]toolScore, 0, len(tools))
	for _, tool := range tools {
		score := scoreTool(tool.Description, tool.InputSchema, role)
		if score <= 0 {
			continue
		}
		scored = append(scored, toolScore{name: tool.Name, score: score})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].name < scored[j].name
	})
	if limit > 0 && len(scored) > limit {
		scored = scored[:limit]
	}
	return scored
}

// scoreTool grades how strongly a description names the given role. The input
// schema refines it: a tool that pages rows is never the report listing, no
// matter what its prose mentions.
func scoreTool(description string, schema json.RawMessage, role string) int {
	text := normalizeToolText(description)
	score := phraseScore(text, role)
	if score == 0 {
		return 0
	}
	score += schemaScore(schemaFields(schema), role)
	if score < 0 {
		return 0
	}
	return score
}

func normalizeToolText(description string) string {
	return strings.Join(strings.Fields(description), "")
}

// phraseScore weights where the phrase sits. The mapping is per role so a
// statistics summary can never satisfy the query role.
func phraseScore(text, role string) int {
	var leading, legacy, weak string
	switch role {
	case toolRoleReports:
		leading, legacy, weak = phraseReportsNew, phraseReportsLegacy, "报表列表"
	case toolRoleQuery:
		leading, legacy, weak = phraseQueryNew, phraseQueryShort, ""
	case toolRoleStatistics:
		leading, legacy, weak = phraseStatsNew, phraseStatsShort, ""
	default:
		return 0
	}

	head := text
	if len(head) > headWindowBytes {
		head = head[:headWindowBytes]
	}
	switch {
	case leading != "" && strings.HasPrefix(text, leading):
		return scorePhraseLeading
	case legacy != "" && strings.HasPrefix(text, legacy):
		return scorePhraseLeading - 10
	case leading != "" && strings.Contains(head, leading):
		return scorePhraseNearHead
	case legacy != "" && strings.Contains(head, legacy):
		return scorePhraseNearHead - 10
	case leading != "" && strings.Contains(text, leading):
		return scorePhraseMention
	case legacy != "" && strings.Contains(text, legacy):
		return scorePhraseMention - 10
	case weak != "" && strings.Contains(text, weak):
		return scorePhraseFallback
	}
	return 0
}

// schemaFields returns the lower-cased property names a tool declares. It is
// nil when the gateway omitted or malformed the schema, which callers must treat
// as "no opinion" rather than "matches nothing".
func schemaFields(schema json.RawMessage) map[string]bool {
	if len(schema) == 0 {
		return nil
	}
	var parsed struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schema, &parsed); err != nil || len(parsed.Properties) == 0 {
		return nil
	}
	fields := make(map[string]bool, len(parsed.Properties))
	for name := range parsed.Properties {
		fields[strings.ToLower(name)] = true
	}
	return fields
}

func schemaScore(fields map[string]bool, role string) int {
	if fields == nil {
		return 0
	}
	switch role {
	case toolRoleReports:
		score := 0
		if fields["designtype"] {
			score += 40
		}
		if fields["includedisplayfields"] || fields["includefilterfields"] || fields["namelike"] {
			score += 30
		}
		// Only data tools page rows. Without designType this cannot be the
		// listing tool, and the penalty is large enough to sink a description
		// that merely mentions the listing summary.
		if !fields["designtype"] && (fields["page"] || fields["pagesize"]) {
			score -= 250
		}
		return score
	case toolRoleQuery, toolRoleStatistics:
		score := 0
		if fields["page"] || fields["pagesize"] {
			score += 40
		}
		if fields["conditions"] {
			score += 20
		}
		if fields["id"] {
			score += 10
		}
		return score
	}
	return 0
}

// legacyReportTool finds the pre-report-runtime single tool, if present.
func legacyReportTool(tools []mcpTool) string {
	candidates := make([]string, 0, 1)
	for _, tool := range tools {
		text := strings.Join(strings.Fields(tool.Description), "")
		if strings.Contains(text, phraseLegacyReport) {
			candidates = append(candidates, tool.Name)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	sort.Strings(candidates)
	return candidates[0]
}
