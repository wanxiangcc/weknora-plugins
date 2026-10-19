package seeyon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestClientReadsPlainJSONAndSSE(t *testing.T) {
	for _, sse := range []bool{false, true} {
		name := "json"
		if sse {
			name = "sse"
		}
		t.Run(name, func(t *testing.T) {
			gw := newFakeGateway(t)
			gw.sse = sse
			gw.addTool("sy-abc", "some tool", func(map[string]interface{}) (string, bool) {
				return `{"code":200,"message":"ok","data":{"value":42}}`, false
			})

			cli, err := newClient(gw.url(), testToken)
			if err != nil {
				t.Fatalf("newClient: %v", err)
			}
			if err := cli.initialize(context.Background()); err != nil {
				t.Fatalf("initialize: %v", err)
			}
			tools, err := cli.listTools(context.Background())
			if err != nil {
				t.Fatalf("listTools: %v", err)
			}
			if len(tools) != 1 || tools[0].Name != "sy-abc" || tools[0].Description != "some tool" {
				t.Fatalf("tools/list = %+v", tools)
			}
			payload, err := cli.callTool(context.Background(), "sy-abc", nil)
			if err != nil {
				t.Fatalf("callTool: %v", err)
			}
			var decoded struct {
				Code int `json:"code"`
				Data struct {
					Value int `json:"value"`
				} `json:"data"`
			}
			if err := json.Unmarshal(payload, &decoded); err != nil {
				t.Fatalf("the tool payload must be the parsed OA envelope, got %s: %v", payload, err)
			}
			if decoded.Code != 200 || decoded.Data.Value != 42 {
				t.Fatalf("payload = %s", payload)
			}
		})
	}
}

func TestClientClassifiesAuthorizationRejection(t *testing.T) {
	gw := newFakeGateway(t)
	gw.addTool("sy-abc", "some tool", nil)

	cli, err := newClient(gw.url(), "WRONG-CODE")
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	err = cli.initialize(context.Background())
	if !errors.Is(err, errAuthorizationRejected) {
		t.Fatalf("want errAuthorizationRejected, got %v", err)
	}
	// The operator needs to know where to get a new code.
	if !strings.Contains(err.Error(), "任务令牌") {
		t.Fatalf("error %q must point at the V5 token page", err)
	}
}

func TestClientClassifiesToolNotAuthorized(t *testing.T) {
	gw := newFakeGateway(t)
	gw.addTool(toolIDStats, descStats, func(map[string]interface{}) (string, bool) {
		// This is verbatim what the gateway returns when the MCP application is
		// not authorized for the 报表管理 tool group.
		return "工具未被允许调用: " + toolIDStats, true
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	_, err = cli.callTool(context.Background(), toolIDStats, nil)
	if !errors.Is(err, errToolNotAuthorized) {
		t.Fatalf("want errToolNotAuthorized, got %v", err)
	}
}

func TestClientSurfacesOtherToolFailures(t *testing.T) {
	gw := newFakeGateway(t)
	gw.addTool("sy-abc", "some tool", func(map[string]interface{}) (string, bool) {
		return "报表不存在", true
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	_, err = cli.callTool(context.Background(), "sy-abc", nil)
	if err == nil {
		t.Fatal("want an error")
	}
	if errors.Is(err, errToolNotAuthorized) {
		t.Fatalf("a plain tool failure must not be reported as an authorization problem: %v", err)
	}
	if !strings.Contains(err.Error(), "报表不存在") {
		t.Fatalf("the gateway's own message must be preserved, got %v", err)
	}
}

func TestClientSurfacesRPCError(t *testing.T) {
	gw := newFakeGateway(t)
	gw.rpcErrors["tools/list"] = &rpcError{Code: -32601, Message: "method not found"}

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	_, err = cli.listTools(context.Background())
	if err == nil || !strings.Contains(err.Error(), "method not found") {
		t.Fatalf("want the gateway's message, got %v", err)
	}
}

func TestExtractJSONPayload(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    string
		wantErr bool
	}{
		{name: "plain json", body: `{"jsonrpc":"2.0"}`, want: `{"jsonrpc":"2.0"}`},
		{name: "leading whitespace", body: "  \n{\"a\":1}\n", want: `{"a":1}`},
		{
			name: "sse frame",
			body: "event: message\ndata: {\"a\":1}\n\n",
			want: `{"a":1}`,
		},
		{
			name: "sse keeps the last data frame",
			body: "data: {\"a\":1}\n\ndata: {\"a\":2}\n\n",
			want: `{"a":2}`,
		},
		{name: "empty", body: "   ", wantErr: true},
		{name: "html error page", body: "<html>502</html>", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractJSONPayload([]byte(tc.body), "text/plain")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want an error, got %s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("extractJSONPayload: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("payload = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClientRejectsNonOKStatus(t *testing.T) {
	// The gateway always answers 200, so anything else is a proxy/edge problem
	// in front of it and must be reported with the status and a body excerpt.
	gw := newFakeGateway(t)
	gw.addTool("sy-abc", "some tool", nil)
	gw.forceStatus = http.StatusBadGateway

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	_, err = cli.listTools(context.Background())
	if err == nil {
		t.Fatal("want an error for a non-200 response")
	}
	for _, want := range []string{"unexpected HTTP 502", "bad gateway"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q must mention %q", err, want)
		}
	}
}

func TestResolveToolsMatchesReportRuntimeByDescription(t *testing.T) {
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	want := map[string]string{
		toolRoleReports:    toolIDReports,
		toolRoleQuery:      toolIDQuery,
		toolRoleStatistics: toolIDStats,
	}
	for role, name := range want {
		if cli.tools[role] != name {
			t.Fatalf("role %s resolved to %q, want %q", role, cli.tools[role], name)
		}
	}
	if cli.legacyTool != "" {
		t.Fatalf("the report-runtime tools must not be mistaken for the legacy tool, got %q", cli.legacyTool)
	}
}

func TestResolveToolsIgnoresUnrelatedTools(t *testing.T) {
	gw := newFakeGateway(t)
	// Every one of these mentions 报表 or is a plausible neighbour; none is a
	// report data tool, so resolution must fail rather than pick one.
	gw.addTool("a", "获取表单浏览态下的报表穿透数据", nil)
	gw.addTool("b", "分页获取明细表数据，来自上游列表、报表穿透结果", nil)
	gw.addTool("c", "获取指标详情", nil)
	gw.addTool("d", "查询表单数据列表", nil)

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	err = cli.resolveTools(context.Background(), nil)
	if !errors.Is(err, errToolNotFound) {
		t.Fatalf("want errToolNotFound, got %v", err)
	}
	// The message has to name the plausible cause and the escape hatch.
	for _, want := range []string{"reports", "query", "statistics", "tool_overrides"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q must mention %q", err, want)
		}
	}
}

func TestResolveToolsFallsBackToLegacySingleTool(t *testing.T) {
	// An OA build predating the report-runtime split exposes one report tool
	// that dispatches on designType.
	gw := newFakeGateway(t)
	gw.addTool(toolIDLegacy, descLegacy, nil)
	// The listing tool has always been separate from the data tool.
	gw.addTool("legacy-reports", "获取用户有权限的报表列表", nil)
	gw.addTool("unrelated", "获取当前用户信息", nil)

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	if cli.tools[toolRoleQuery] != toolIDLegacy || cli.tools[toolRoleStatistics] != toolIDLegacy {
		t.Fatalf("the legacy tool must serve both data roles, got %v", cli.tools)
	}
	if cli.legacyTool != toolIDLegacy {
		t.Fatalf("legacyTool = %q, want %q", cli.legacyTool, toolIDLegacy)
	}
}

func TestResolveToolsHonoursOverrides(t *testing.T) {
	gw := newFakeGateway(t)
	gw.addTool("sy-pinned", "某个被改过描述的工具", nil)

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	overrides := map[string]string{
		toolRoleReports:    "sy-pinned",
		toolRoleQuery:      "sy-pinned",
		toolRoleStatistics: "sy-pinned",
	}
	if err := cli.resolveTools(context.Background(), overrides); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	for _, role := range toolRoles {
		if cli.tools[role] != "sy-pinned" {
			t.Fatalf("role %s = %q, want the pinned name", role, cli.tools[role])
		}
	}
}

// The gateway's tool description is the operation summary *plus* prose naming
// the prerequisite tools, so a plain substring match hands the listing role to
// the query data tool. Reproduced from production: the listing call then fails
// with 报表ID和报表名称至少传入一个 because a listing request has no report id.
func TestScoreToolPrefersTheToolWhoseOwnSummaryItIs(t *testing.T) {
	const reports = "获取有权限的报表列表"
	const queryWithPrerequisiteMention = "搜索查询报表数据。先调用获取有权限的报表列表拿到报表ID。参数：id、page、pageSize"
	schemaOf := func(names ...string) json.RawMessage {
		props := make(map[string]interface{}, len(names))
		for _, name := range names {
			props[name] = map[string]string{"type": "string"}
		}
		raw, err := json.Marshal(map[string]interface{}{
			"type": "object", "properties": props,
		})
		if err != nil {
			t.Fatalf("marshal schema: %v", err)
		}
		return raw
	}

	tools := []mcpTool{
		{Name: "query", Description: queryWithPrerequisiteMention,
			InputSchema: schemaOf("id", "page", "pageSize", "conditions")},
		{Name: "reports", Description: reports,
			InputSchema: schemaOf("designType", "includeDisplayFields")},
	}
	if name, _ := bestToolFor(tools, toolRoleReports); name != "reports" {
		t.Fatalf("reports role = %q, want the listing tool", name)
	}
	if name, _ := bestToolFor(tools, toolRoleQuery); name != "query" {
		t.Fatalf("query role = %q, want the data tool", name)
	}

	// Even with the listing tool's summary missing entirely, the schema alone
	// must keep the paging tool out of the listing role.
	if name, _ := bestToolFor(tools[1:], toolRoleReports); name != "reports" {
		t.Fatalf("reports role with one candidate = %q", name)
	}
	mention := scoreTool(queryWithPrerequisiteMention, nil, toolRoleReports)
	own := scoreTool(reports, nil, toolRoleReports)
	if own <= mention {
		t.Fatalf("own summary scored %d, a mention scored %d; the summary must win", own, mention)
	}
}

func TestScoreToolKeepsRolesApart(t *testing.T) {
	cases := []struct {
		description string
		role        string
		wantZero    bool
	}{
		{descReports, toolRoleReports, false},
		{"获取用户有权限的报表列表", toolRoleReports, false},
		{"获取所有报表列表", toolRoleReports, false},
		{descQuery, toolRoleQuery, false},
		{"按条件查询报表数据", toolRoleQuery, false},
		{descStats, toolRoleStatistics, false},
		{"统计报表数据导出", toolRoleStatistics, false},
		// Cross-role: the statistics summary must not satisfy the query role.
		{descStats, toolRoleQuery, true},
		{descQuery, toolRoleStatistics, true},
		{descReports, toolRoleQuery, true},
		{descReports, toolRoleStatistics, true},
		{"获取当前用户信息", toolRoleReports, true},
		{"获取表单详情数据", toolRoleQuery, true},
		{descLegacy, toolRoleQuery, true},
	}
	for _, tc := range cases {
		got := scoreTool(tc.description, nil, tc.role)
		if tc.wantZero && got != 0 {
			t.Fatalf("scoreTool(%q, %q) = %d, want 0", tc.description, tc.role, got)
		}
		if !tc.wantZero && got <= 0 {
			t.Fatalf("scoreTool(%q, %q) = %d, want a positive score", tc.description, tc.role, got)
		}
	}
}

// A gateway that omits inputSchema must keep working: the text score alone
// still separates the roles.
func TestScoreToolWorksWithoutASchema(t *testing.T) {
	tools := []mcpTool{
		{Name: "reports", Description: descReports},
		{Name: "query", Description: descQuery},
		{Name: "stats", Description: descStats},
	}
	want := map[string]string{
		toolRoleReports:    "reports",
		toolRoleQuery:      "query",
		toolRoleStatistics: "stats",
	}
	for role, name := range want {
		if got, _ := bestToolFor(tools, role); got != name {
			t.Fatalf("role %s = %q, want %q", role, got, name)
		}
	}
}

func TestResolveToolsPicksDeterministicallyAmongTies(t *testing.T) {
	// Two tools with the same score must resolve to the same name on every run,
	// because tool resolution happens on each sync and an unstable choice would
	// make the sync path depend on map iteration order.
	tools := []mcpTool{
		{Name: "zzz", Description: descQuery},
		{Name: "aaa", Description: descQuery},
	}
	for i := 0; i < 20; i++ {
		resolved, err := (&client{}).resolveToolsFrom(context.Background(), tools, map[string]string{
			toolRoleReports:    "r",
			toolRoleStatistics: "s",
		})
		if err != nil {
			t.Fatalf("resolveToolsFrom: %v", err)
		}
		if resolved[toolRoleQuery] != "aaa" {
			t.Fatalf("ties must break on the lowest name, got %q", resolved[toolRoleQuery])
		}
	}
}

func TestNewClientValidatesInputs(t *testing.T) {
	if _, err := newClient("https://example.com/mcp", "  "); err == nil {
		t.Fatal("a blank access token must be rejected")
	}
	if _, err := newClient("ftp://example.com/mcp", testToken); err == nil {
		t.Fatal("a non-http scheme must be rejected")
	}
	if _, err := newClient("  ", testToken); err == nil {
		t.Fatal("a blank base_url must be rejected: the connector has no default gateway")
	}
	cli, err := newClient("  "+testGatewayURL+"  ", testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if cli.baseURL != testGatewayURL {
		t.Fatalf("baseURL = %q, want %q", cli.baseURL, testGatewayURL)
	}
}

func TestCallToolRejectsEmptyName(t *testing.T) {
	cli := &client{}
	_, err := cli.callTool(context.Background(), "  ", nil)
	if !errors.Is(err, errToolNotFound) {
		t.Fatalf("want errToolNotFound, got %v", err)
	}
}

func TestPayloadFromPrefersStructuredContent(t *testing.T) {
	// The gateway mirrors the same object into structuredContent; either source
	// must yield the same payload.
	call := callToolResult{
		Content:           []contentItem{{Type: "text", Text: `{"code":200,"data":{"a":1}}`}},
		StructuredContent: json.RawMessage(`{"code":200,"data":{"a":2}}`),
	}
	if got := string(payloadFrom(call, call.Content[0].Text)); got != `{"code":200,"data":{"a":2}}` {
		t.Fatalf("payload = %s", got)
	}

	// A null structuredContent falls back to the text item.
	call.StructuredContent = json.RawMessage("null")
	if got := string(payloadFrom(call, call.Content[0].Text)); got != `{"code":200,"data":{"a":1}}` {
		t.Fatalf("payload = %s", got)
	}

	// Non-JSON text yields nothing rather than a bogus payload.
	call.StructuredContent = nil
	call.Content[0].Text = "工具未被允许调用"
	if got := payloadFrom(call, call.Content[0].Text); got != nil {
		t.Fatalf("payload = %s, want nil", got)
	}
}

func TestTruncateForErrorKeepsMessagesShort(t *testing.T) {
	long := strings.Repeat("x", 4096)
	got := truncateForError([]byte(long))
	if len(got) > 520 {
		t.Fatalf("truncated body is still %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("a truncated body must be marked: %q", got[len(got)-8:])
	}
	if short := truncateForError([]byte("ok")); short != "ok" {
		t.Fatalf("short bodies must pass through, got %q", short)
	}
}

func TestFakeGatewayRecordsCalls(t *testing.T) {
	// Guards the fixture itself: the tests below rely on call recording to prove
	// the query tool is used for query reports and the statistics tool for
	// statistics reports.
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if _, err := cli.callTool(context.Background(), toolIDQuery, map[string]interface{}{"id": "1"}); err != nil {
		t.Fatalf("callTool: %v", err)
	}
	got := gw.calledToolNames()
	if len(got) != 1 || got[0] != toolIDQuery {
		t.Fatalf("recorded calls = %v", got)
	}
	if fmt.Sprint(gw.recordedCalls()[0]) != "tools/call:"+toolIDQuery {
		t.Fatalf("recorded call = %v", gw.recordedCalls())
	}
}

// When the gateway rejects the resolved listing tool, the runner-up must be
// tried: the operator asked for a connectivity test, not for the gateway's
// opaque parameter error.
func TestListReportIndexFallsBackToTheRunnerUp(t *testing.T) {
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	// Simulate the mis-resolution: the pinned winner is not a listing tool at
	// all (the gateway answers an error for it), and the real one is second.
	cli.tools[toolRoleReports] = "0-not-a-listing-tool"
	cli.available = []mcpTool{
		{Name: "0-not-a-listing-tool", Description: descReports},
		{Name: toolIDReports, Description: descReports},
	}

	index, err := listReportIndex(context.Background(), cli, []string{designTypeQuery})
	if err != nil {
		t.Fatalf("listReportIndex: %v", err)
	}
	if len(index) == 0 {
		t.Fatal("the fallback returned an empty index")
	}
	if cli.tools[toolRoleReports] != toolIDReports {
		t.Fatalf("reports tool = %q, want the fallback %q", cli.tools[toolRoleReports], toolIDReports)
	}
}

// When every candidate fails the error must name the actionable cause.
func TestListReportIndexExplainsARejectedListing(t *testing.T) {
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	cli.tools[toolRoleReports] = "0-missing"
	cli.available = []mcpTool{{Name: "0-missing", Description: descReports}}

	_, err = listReportIndex(context.Background(), cli, []string{designTypeQuery})
	if !errors.Is(err, errToolNotFound) {
		t.Fatalf("err = %v, want it to wrap errToolNotFound", err)
	}
	if !strings.Contains(err.Error(), "0-missing") {
		t.Fatalf("error %q must name the tool it tried", err)
	}
}
