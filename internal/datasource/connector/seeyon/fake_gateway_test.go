package seeyon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

// Tool identifiers as comi-mcp actually emits them. They are 32-hex ids
// (history tools keep a fixed id instead of a "sy-"/"cs-" + MD5 name), which is
// exactly why the connector resolves them at run time instead of hard-coding
// them — using the real values here keeps the fixtures honest.
const (
	toolIDReports  = "26920c6b49bd7b132c8f22cbf50c6224"
	toolIDQuery    = "d3bd5b44029d387ab2f51cf64b58500c"
	toolIDStats    = "8605412006967adff620f611c78c8f87"
	toolIDLegacy   = "847e7d99b8a30ddb09afdac54941484b"
	toolIDSummary  = "efc2ebce83125822360132fb42f7909a"
	toolIDOpenForm = "53a2ba567cef3fd8614d1ab20a210060"
)

// The @Operation summaries the gateway exposes as tool descriptions.
const (
	descReports  = "获取有权限的报表列表"
	descQuery    = "搜索查询报表数据"
	descStats    = "搜索统计报表数据"
	descLegacy   = "获取报表具体数据"
	descSummary  = "获取指标详情"
	descConfirm  = "中危/高危工具二次确认"
	descOpenForm = "获取表单详情数据"
)

const testToken = "TEST-MCP-AUTH-CODE"

// testGatewayURL stands in for the operator's own gateway endpoint. The
// connector deliberately ships no default URL, so every config needs one.
// Loopback keeps the config tests independent of public DNS and of the SSRF
// policy's view of external hosts; TestMain whitelists it.
const testGatewayURL = "http://127.0.0.1:8080/mcp-gateway/1/apps/seeyon/message"

// TestMain whitelists loopback so the httptest gateway is reachable under the
// production SSRF policy. Production itself keeps the strict default, so a
// gateway on a private network still has to be listed in SSRF_WHITELIST_EXTRA.
func TestMain(m *testing.M) {
	_ = os.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	os.Exit(m.Run())
}

// toolHandler serves one tools/call invocation: it returns the stringified JSON
// the gateway puts into content[0].text, plus whether the tool refused.
type toolHandler func(args map[string]interface{}) (text string, isError bool)

// fakeGateway models the Seeyon MCP gateway: a stateless JSON-RPC 2.0 endpoint
// that answers every request with HTTP 200, carries tool output as a JSON
// *string* inside content[0].text, and reports failures either as a JSON-RPC
// error object or as result.isError.
type fakeGateway struct {
	t   *testing.T
	srv *httptest.Server

	mu       sync.Mutex
	tools    []mcpTool
	handlers map[string]toolHandler
	// sse makes every response an SSE frame, which is what the gateway does when
	// the client offers text/event-stream.
	sse bool
	// rpcErrors injects a JSON-RPC error per method.
	rpcErrors map[string]*rpcError
	// refusedTools makes tools/list omit the given names, modelling a gateway
	// app that is not authorized for a tool group.
	refusedTools map[string]bool
	// forceStatus, when non-zero, answers every request with that HTTP status,
	// modelling a proxy or edge failure in front of the gateway.
	forceStatus int
	// calls records "method" or "tools/call:<name>" in order.
	calls []string
	// unfilteredListings counts report listings sent with an empty nameLike,
	// i.e. the expensive "give me every report" shape. Tests assert it stays 0
	// on the picker paths.
	unfilteredListings int
	// dataCalls counts calls to a report *data* tool (query/statistics/legacy).
	dataCalls int
	// reportIDs is the order newFakeReportGateway published, so newTestConfig
	// can select one report. Selection is mandatory and single, so a config
	// without a resource id would fail before reaching any tool.
	reportIDs []string
	// requireTicket models the gateway application's 中危/高危 gate: the report
	// tools refuse to run without a single-use _confirmUuid.
	requireTicket bool
	// tickets maps an issued ticket to the tool it was issued for. A ticket is
	// deleted on first use, matching the gateway's single-use contract.
	tickets   map[string]string
	ticketSeq int
}

// ticketTools are the tools the gateway application classifies as 中危/高危.
var ticketTools = map[string]bool{
	toolIDReports: true,
	toolIDQuery:   true,
	toolIDStats:   true,
	toolIDLegacy:  true,
}

func newFakeGateway(t *testing.T) *fakeGateway {
	t.Helper()
	g := &fakeGateway{
		t:            t,
		handlers:     map[string]toolHandler{},
		rpcErrors:    map[string]*rpcError{},
		refusedTools: map[string]bool{},
		tickets:      map[string]string{},
	}
	g.srv = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.srv.Close)
	return g
}

func (g *fakeGateway) url() string { return g.srv.URL + "/mcp-gateway/1/apps/seeyon/message" }

// counters reports how many expensive shapes the gateway was asked for.
func (g *fakeGateway) counters() (unfilteredListings, dataCalls int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.unfilteredListings, g.dataCalls
}

// countDataCall records that a report data tool was invoked, so a test can prove
// a path never probes report data (a one-row probe still costs seconds on a
// report with hundreds of thousands of rows).
func (g *fakeGateway) countDataCall(name string) {
	switch name {
	case toolIDQuery, toolIDStats, toolIDLegacy, toolIDSummary, toolIDOpenForm:
		g.mu.Lock()
		g.dataCalls++
		g.mu.Unlock()
	}
}

func (g *fakeGateway) addTool(id, description string, handler toolHandler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tools = append(g.tools, mcpTool{Name: id, Description: description})
	if handler != nil {
		g.handlers[id] = handler
	}
}

func (g *fakeGateway) recordedCalls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.calls...)
}

// calledToolNames returns the tool names that were invoked, in order.
func (g *fakeGateway) calledToolNames() []string {
	var out []string
	for _, call := range g.recordedCalls() {
		if name, ok := strings.CutPrefix(call, "tools/call:"); ok {
			out = append(out, name)
		}
	}
	return out
}

func (g *fakeGateway) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var request struct {
		ID     int    `json:"id"`
		Method string `json:"method"`
		Params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		} `json:"params"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		g.t.Errorf("fake gateway received an undecodable request: %v", err)
		return
	}

	g.mu.Lock()
	call := request.Method
	if request.Method == "tools/call" {
		call += ":" + request.Params.Name
	}
	g.calls = append(g.calls, call)
	rpcErr := g.rpcErrors[request.Method]
	forceStatus := g.forceStatus
	g.mu.Unlock()

	if forceStatus != 0 {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(forceStatus)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
		return
	}

	// The real gateway authenticates the personal authorization code and, on
	// failure, still answers HTTP 200 with a JSON-RPC error.
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		g.write(w, nil, &rpcError{Code: -32603, Message: "授权码无效或已过期，请重新获取"})
		return
	}
	if rpcErr != nil {
		g.write(w, nil, rpcErr)
		return
	}

	switch request.Method {
	case "initialize":
		g.write(w, map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"serverInfo":      map[string]interface{}{"name": "seeyon-mcp-gateway", "version": "1.0"},
		}, nil)
	case "tools/list":
		g.write(w, map[string]interface{}{"tools": g.visibleTools()}, nil)
	case "tools/call":
		g.countDataCall(request.Params.Name)
		handler, ok := g.handler(request.Params.Name)
		if !ok {
			g.write(w, nil, &rpcError{Code: -32602, Message: "工具不存在: " + request.Params.Name})
			return
		}
		if err := g.checkTicket(request.Params.Name, request.Params.Arguments); err != nil {
			g.write(w, nil, err)
			return
		}
		text, isError := handler(request.Params.Arguments)
		g.write(w, map[string]interface{}{
			"content": []map[string]interface{}{{"type": "text", "text": text}},
			"isError": isError,
		}, nil)
	default:
		g.write(w, nil, &rpcError{Code: -32601, Message: "method not found: " + request.Method})
	}
}

// checkTicket enforces the two-step confirmation gate the gateway application
// applies to its 中危/高危 tools.
func (g *fakeGateway) checkTicket(name string, args map[string]interface{}) *rpcError {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.requireTicket || !ticketTools[name] {
		return nil
	}
	uuid, _ := args["_confirmUuid"].(string)
	if strings.TrimSpace(uuid) == "" {
		return &rpcError{Code: -32600, Message: "中危/高危工具调用缺少二次确认 UUID"}
	}
	bound, ok := g.tickets[uuid]
	if !ok || bound != name {
		return &rpcError{Code: -32600, Message: "二次确认 UUID 无效、已使用或不匹配"}
	}
	// Single use: the gateway invalidates the ticket as it is consumed.
	delete(g.tickets, uuid)
	return nil
}

func (g *fakeGateway) issueTicket(toolName string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ticketSeq++
	uuid := fmt.Sprintf("ticket-%d", g.ticketSeq)
	g.tickets[uuid] = toolName
	return uuid
}

func (g *fakeGateway) visibleTools() []mcpTool {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]mcpTool, 0, len(g.tools))
	for _, tool := range g.tools {
		if g.refusedTools[tool.Name] {
			continue
		}
		out = append(out, tool)
	}
	return out
}

func (g *fakeGateway) handler(name string) (toolHandler, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	handler, ok := g.handlers[name]
	return handler, ok
}

func (g *fakeGateway) write(w http.ResponseWriter, result interface{}, rpcErr *rpcError) {
	response := map[string]interface{}{"jsonrpc": "2.0", "id": 1}
	if rpcErr != nil {
		response["error"] = rpcErr
	} else {
		response["result"] = result
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		g.t.Errorf("fake gateway cannot encode a response: %v", err)
		return
	}
	if g.sse {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message\ndata: " + string(encoded) + "\n\n"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(encoded)
}

// fakeReport is one report the fake gateway serves.
type fakeReport struct {
	ID         string
	Name       string
	DesignType string
	Category   string
	Columns    []string
	ColumnIDs  []string
	Rows       [][]string
	// statShape serves rows in the statistics encoding
	// ({"values":[…], …}) instead of the query encoding ([…]).
	statShape bool
	// Filters are the report's filterable fields, exposed when the listing asks
	// for filter field definitions.
	Filters []fakeFilterField
	// ignoreEveryCondition makes the fake drop every condition the way the real
	// runtime drops one it cannot apply. The connector must refuse to sync
	// rather than read the unfiltered report.
	ignoreEveryCondition bool
	// FilterByChangeProbe models the runtime's change query: when the connector
	// asks "which rows changed after <watermark>", answer with ProbeChanged rows
	// and no data. Only set it in tests that exercise the change probe.
	FilterByChangeProbe bool
	// ProbeChanged is the number of rows the change query reports.
	ProbeChanged int
	// FailOnPageAfter makes the fake fail every page after N (1 = only page 1
	// succeeds), modelling a read that dies midway.
	FailOnPageAfter int
}

// fakeFilterField is one filterable field of a fake report.
type fakeFilterField struct {
	TableName    string
	FieldName    string
	FieldDisplay string
	InputType    string
	IsEnum       bool
	Operators    []string
	EnumOptions  []map[string]string
}

// displayFields renders the column definitions the discovery tool returns for a
// report. Query reports have them; statistics reports genuinely have none, so
// the empty slice is the faithful answer.
func (r *fakeReport) displayFields() []map[string]interface{} {
	if r.statShape {
		return []map[string]interface{}{}
	}
	out := make([]map[string]interface{}, 0, len(r.ColumnIDs))
	for i, id := range r.ColumnIDs {
		table, field := id, ""
		if cut := strings.LastIndex(id, "_"); cut > 0 {
			table, field = id[:cut], id[cut+1:]
		}
		label := ""
		if i < len(r.Columns) {
			label = r.Columns[i]
		}
		out = append(out, map[string]interface{}{
			"tableName":    table,
			"fieldName":    field,
			"fieldDisplay": label,
			"index":        i,
		})
	}
	return out
}

// filterFields renders the report's filterable definitions, with one synthetic
// field so the fixed-filter path always has something to exercise.
func (r *fakeReport) filterFields() []map[string]interface{} {
	filters := r.Filters
	if filters == nil && len(r.ColumnIDs) > 0 {
		table, field := r.ColumnIDs[0], ""
		if cut := strings.LastIndex(r.ColumnIDs[0], "_"); cut > 0 {
			table, field = r.ColumnIDs[0][:cut], r.ColumnIDs[0][cut+1:]
		}
		filters = []fakeFilterField{{
			TableName: table, FieldName: field, FieldDisplay: "部门",
			InputType: "text", Operators: []string{"Equal", "NotEqual", "Like"},
		}}
	}
	out := make([]map[string]interface{}, 0, len(filters))
	for _, filter := range filters {
		entry := map[string]interface{}{
			"tableName":    filter.TableName,
			"fieldName":    filter.FieldName,
			"fieldDisplay": filter.FieldDisplay,
			"inputType":    filter.InputType,
			"isEnum":       filter.IsEnum,
			"operators":    filter.Operators,
		}
		if len(filter.EnumOptions) > 0 {
			entry["enumOptions"] = filter.EnumOptions
		}
		out = append(out, entry)
	}
	return out
}

func (r *fakeReport) designTypeName() string {
	switch r.DesignType {
	case designTypeQuery, designTypeWeaQuery:
		return "查询报表"
	case designTypeStatistics, designTypeMultStats, designTypeScheduledStats, designTypeWeaStats:
		return "统计报表"
	default:
		return r.DesignType
	}
}

// drillFields renders the drill metadata a statistics row carries: one entry per
// column, with the table/field split at the last underscore (the table name
// itself contains underscores).
func (r *fakeReport) drillFields() []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(r.ColumnIDs))
	for i, id := range r.ColumnIDs {
		table, field := id, ""
		if cut := strings.LastIndex(id, "_"); cut > 0 {
			table, field = id[:cut], id[cut+1:]
		}
		label := ""
		if i < len(r.Columns) {
			label = r.Columns[i]
		}
		out = append(out, map[string]interface{}{
			"tableName":    table,
			"fieldName":    field,
			"fieldDisplay": label,
			"fieldRole":    fieldRoleRowHeader,
		})
	}
	return out
}

// page renders the QueryReportRes/QueryStatisticsRes envelope for one page.
// knownFields is the set of "<table>.<field>" names the runtime can filter on:
// the report's own columns plus the CAP4 system fields, which are filterable
// even though they are never displayed.
func (r *fakeReport) knownFields() map[string]bool {
	known := make(map[string]bool)
	for _, id := range r.ColumnIDs {
		if idx := strings.LastIndex(id, "_"); idx > 0 {
			known[id[:idx]+"."+id[idx+1:]] = true
		}
	}
	for _, field := range r.Filters {
		known[field.TableName+"."+field.FieldName] = true
	}
	return known
}

func (r *fakeReport) page(args map[string]interface{}) (string, bool) {
	page := argInt(args, "page", 1)
	size := argInt(args, "pageSize", defaultPageSize)
	if size <= 0 {
		size = defaultPageSize
	}
	// Change probe: a date condition on a system marker field is answered with
	// the number of rows modified after the watermark, without data.
	if r.FilterByChangeProbe && hasChangeCondition(args) {
		envelope := map[string]interface{}{
			"code":    "200",
			"message": "操作成功",
			"data": map[string]interface{}{
				"id":           r.ID,
				"name":         r.Name,
				"designType":   r.DesignType,
				"page":         strconv.Itoa(page),
				"pageSize":     strconv.Itoa(size),
				"totalRecords": strconv.Itoa(r.ProbeChanged),
				"totalPages":   strconv.Itoa(r.ProbeChanged),
				"data": map[string]interface{}{
					"columns":   r.Columns,
					"columnIds": nil,
					"rows":      []interface{}{},
					"ids":       []string{},
				},
			},
		}
		return mustJSON(envelope), false
	}

	start := (page - 1) * size
	if start > len(r.Rows) {
		start = len(r.Rows)
	}
	end := start + size
	if end > len(r.Rows) {
		end = len(r.Rows)
	}
	slice := r.Rows[start:end]

	rows := make([]interface{}, 0, len(slice))
	for _, row := range slice {
		values := make([]interface{}, 0, len(row))
		for _, cell := range row {
			values = append(values, cell)
		}
		if r.statShape {
			// Statistics rows carry their column identity in drillContext;
			// columnIds is null in this shape, exactly as the runtime behaves.
			rows = append(rows, map[string]interface{}{
				"values":       values,
				"drillContext": map[string]interface{}{"fieldValues": r.drillFields()},
			})
			continue
		}
		rows = append(rows, values)
	}
	ids := make([]string, 0, len(slice))
	for i := range slice {
		ids = append(ids, fmt.Sprintf("%s-%d", r.ID, start+i+1))
	}

	total := len(r.Rows)
	totalPages := (total + size - 1) / size
	if totalPages == 0 {
		totalPages = 1
	}

	columns := r.Columns
	if columns == nil {
		columns = []string{}
	}

	// Every pagination field is a *string* in the CAP4 runtime contract, and the
	// gateway application also serializes the envelope code as a string, both of
	// which the connector must tolerate.
	columnIDs := interface{}(r.ColumnIDs)
	if r.statShape {
		columnIDs = nil
	}

	// The runtime does not fail on a condition it cannot apply: it drops the
	// entry and echoes it with a reason, keeping the response otherwise intact.
	// The fake mirrors that so the connector's guard is exercised.
	ignoredConditions := []interface{}{}
	if rawConds, ok := args["conditions"].([]interface{}); ok {
		known := r.knownFields()
		for _, raw := range rawConds {
			entry, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			tableName, _ := entry["tableName"].(string)
			fieldName, _ := entry["fieldName"].(string)
			if r.ignoreEveryCondition || !known[tableName+"."+fieldName] {
				op, _ := entry["op"].(string)
				value, _ := entry["value"].(string)
				ignoredConditions = append(ignoredConditions, map[string]interface{}{
					"tableName": tableName,
					"fieldName": fieldName,
					"op":        op,
					"value":     value,
					"reason":    "FIELD_NOT_FOUND",
				})
			}
		}
	}
	envelope := map[string]interface{}{
		"code":    "200",
		"message": "操作成功",
		"data": map[string]interface{}{
			"id":                r.ID,
			"name":              r.Name,
			"designType":        r.DesignType,
			"page":              strconv.Itoa(page),
			"pageSize":          strconv.Itoa(size),
			"totalRecords":      strconv.Itoa(total),
			"totalPages":        strconv.Itoa(totalPages),
			"ignoredConditions": ignoredConditions,
			"ignoredSorts":      []interface{}{},
			"data": map[string]interface{}{
				"columns":   columns,
				"columnIds": columnIDs,
				"rows":      rows,
				"ids":       ids,
			},
		},
	}
	return mustJSON(envelope), false
}

// newFakeReportGateway wires the three report-runtime tools plus the noise tools
// a real gateway also exposes, so tool resolution is exercised against a
// realistic listing rather than a hand-picked one.
func newFakeReportGateway(t *testing.T, reports ...*fakeReport) *fakeGateway {
	t.Helper()
	g := newFakeGateway(t)

	byID := make(map[string]*fakeReport, len(reports))
	for _, report := range reports {
		byID[report.ID] = report
	}

	for _, report := range reports {
		g.reportIDs = append(g.reportIDs, report.ID)
	}
	sort.Strings(g.reportIDs)

	g.addTool(toolIDReports, descReports, func(args map[string]interface{}) (string, bool) {
		// The gateway application needs designType explicitly: omitting it makes the
		// tool answer 系统内部错误. Mirroring that keeps the connector honest about
		// always sending it.
		designType, _ := args["designType"].(string)
		if strings.TrimSpace(designType) == "" {
			return mustJSON(map[string]interface{}{
				"code": "200", "message": "系统内部错误，请联系管理员", "data": nil,
			}), false
		}
		nameLike, _ := args["nameLike"].(string)
		if strings.TrimSpace(nameLike) == "" {
			g.mu.Lock()
			g.unfilteredListings++
			g.mu.Unlock()
		}
		withDisplay, _ := args["includeDisplayFields"].(bool)
		withFilters, _ := args["includeFilterFields"].(bool)
		list := make([]map[string]interface{}, 0, len(reports))
		for _, report := range reports {
			if report.DesignType != designType {
				continue
			}
			if nameLike != "" && !strings.Contains(report.Name, nameLike) {
				continue
			}
			entry := map[string]interface{}{
				"id":             report.ID,
				"name":           report.Name,
				"designType":     report.DesignType,
				"designTypeName": report.designTypeName(),
				"category":       report.Category,
			}
			if withDisplay {
				entry["displayFields"] = report.displayFields()
			}
			if withFilters {
				entry["filterFields"] = report.filterFields()
			}
			list = append(list, entry)
		}
		return mustJSON(map[string]interface{}{
			"code":    "200",
			"message": "操作成功",
			"data":    map[string]interface{}{"total": len(list), "reports": list},
		}), false
	})

	fetch := func(args map[string]interface{}) (string, bool) {
		id, _ := args["id"].(string)
		report, ok := byID[id]
		if !ok {
			return mustJSON(map[string]interface{}{
				"code": 500, "message": "报表不存在", "data": nil,
			}), false
		}
		return report.page(args)
	}
	g.addTool(toolIDQuery, descQuery, fetch)
	g.addTool(toolIDStats, descStats, fetch)

	// Noise: tools that mention 报表 but are not report data tools, plus an
	// unrelated tool, so the matcher has to score rather than pattern-match.
	g.addTool(toolIDOpenForm, descOpenForm, func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{"code": 200, "data": map[string]interface{}{}}), false
	})
	g.addTool(toolIDSummary, descSummary, func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{"code": 200, "data": map[string]interface{}{}}), false
	})
	g.addTool("9f119828bec06e1274ce91ac99def047", "获取当前用户信息", func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{"code": 200, "data": map[string]interface{}{}}), false
	})
	// The ticket issuer exists on every gateway; requireTicket decides whether a
	// call is actually refused without one.
	g.addTool(confirmToolName, descConfirm, func(args map[string]interface{}) (string, bool) {
		toolName, _ := args["toolName"].(string)
		uuid := g.issueTicket(toolName)
		return mustJSON(map[string]interface{}{
			"code":    "200",
			"message": "操作成功",
			"data": map[string]interface{}{
				"uuid":      uuid,
				"toolName":  toolName,
				"expiresAt": 1764000000000,
			},
		}), false
	})
	return g
}

// newTestConfig builds a data source configuration pointing at the fake gateway.
func newTestConfig(gw *fakeGateway, settings map[string]interface{}) *types.DataSourceConfig {
	credentials := map[string]interface{}{"access_token": testToken}
	if gw != nil {
		credentials["base_url"] = gw.url()
	}
	if settings == nil {
		settings = map[string]interface{}{}
	}
	config := &types.DataSourceConfig{Credentials: credentials, Settings: settings}
	if gw != nil && len(gw.reportIDs) > 0 {
		// A data source syncs exactly one report, so default to the first one the
		// fake gateway published; tests that care about a specific report
		// override this.
		config.ResourceIDs = []string{gw.reportIDs[0]}
	}
	return config
}

func mustJSON(value interface{}) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

// collectHandler is the streaming sink used by the tests: it records items and
// the last checkpoint, so the connector's emit order and cursor commits can be
// asserted directly.
type collectHandler struct {
	items       []types.FetchedItem
	checkpoint  *types.SyncCursor
	checkpoints int
	// failOn makes Emit return an error for the item whose ExternalID matches,
	// modelling the engine abandoning a sync mid-report.
	failOn string
}

func (h *collectHandler) Emit(_ context.Context, item types.FetchedItem) error {
	if h.failOn != "" && item.ExternalID == h.failOn {
		return errors.New("simulated ingest failure")
	}
	h.items = append(h.items, item)
	return nil
}

func (h *collectHandler) Checkpoint(_ context.Context, cursor *types.SyncCursor) error {
	h.checkpoint = cursor
	h.checkpoints++
	return nil
}

// externalIDs returns the emitted ids in order.
func (h *collectHandler) externalIDs() []string {
	out := make([]string, 0, len(h.items))
	for _, item := range h.items {
		out = append(out, item.ExternalID)
	}
	return out
}

// itemByID finds an emitted item.
func (h *collectHandler) itemByID(id string) (types.FetchedItem, bool) {
	for _, item := range h.items {
		if item.ExternalID == id {
			return item, true
		}
	}
	return types.FetchedItem{}, false
}

func argInt(args map[string]interface{}, key string, fallback int) int {
	raw, ok := args[key]
	if !ok {
		return fallback
	}
	if value, ok := settingInt(raw); ok {
		return value
	}
	return fallback
}

// hasChangeCondition reports whether the request asks for rows modified after a
// watermark, i.e. carries a comparison condition on a system date field.
func hasChangeCondition(args map[string]interface{}) bool {
	conds, ok := args["conditions"].([]interface{})
	if !ok {
		return false
	}
	for _, raw := range conds {
		entry, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		field, _ := entry["fieldName"].(string)
		if field == "modify_date" || field == "start_date" {
			return true
		}
	}
	return false
}
