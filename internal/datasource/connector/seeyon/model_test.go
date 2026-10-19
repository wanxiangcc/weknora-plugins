package seeyon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// oneReport returns a single small query report with three columns.
func oneReport() *fakeReport {
	return &fakeReport{
		ID: "1", Name: "示例查询报表", DesignType: designTypeQuery,
		Columns:   []string{"单据号", "部门", "金额"},
		ColumnIDs: []string{"formmain_1_0_field1", "formmain_1_0_field2", "formmain_1_0_field3"},
		Rows:      [][]string{{"NO-1", "研发", "100"}, {"NO-2", "测试", "200"}},
	}
}

// captureArgs wraps a tool handler and records the arguments it was called with.
func captureArgs(gw *fakeGateway, toolID string, into *[]map[string]interface{}) {
	original := gw.handlers[toolID]
	gw.handlers[toolID] = func(args map[string]interface{}) (string, bool) {
		*into = append(*into, args)
		return original(args)
	}
}

func TestSyncSendsTheDesignTypeOnEveryListingCall(t *testing.T) {
	// The gateway answers 系统内部错误 without designType, so the connector
	// must never omit it. The fake refuses an untyped request, which turns a
	// regression into a failure here rather than in production.
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)

	var listings []map[string]interface{}
	captureArgs(gw, toolIDReports, &listings)

	if _, err := NewConnector().ListResources(context.Background(), newTestConfig(gw, nil), ""); err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	if len(listings) == 0 {
		t.Fatal("the listing tool was never called")
	}
	seen := map[string]bool{}
	for _, args := range listings {
		designType, _ := args["designType"].(string)
		if designType == "" {
			t.Fatalf("a listing call omitted designType: %v", args)
		}
		seen[designType] = true
	}
	// One call per configured design type.
	for _, want := range []string{designTypeQuery, designTypeStatistics} {
		if !seen[want] {
			t.Fatalf("no listing call for design type %s: %v", want, listings)
		}
	}
}

func TestSyncObtainsAFreshConfirmationTicketPerCall(t *testing.T) {
	// The report tools are 中危/高危 on the gateway application: every call needs its
	// own single-use ticket. Two pages plus the listing means several tickets, and
	// the gateway consumes each one, so a cached ticket would fail here.
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	gw.requireTicket = true
	ds := newTestConfig(gw, map[string]interface{}{
		"max_document_bytes": float64(minMaxDocumentBytes),
		"page_size":          float64(100),
	})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream with the confirmation gate on: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("nothing was emitted")
	}

	calls := gw.calledToolNames()
	var confirms, sensitive int
	for _, name := range calls {
		switch {
		case name == confirmToolName:
			confirms++
		case ticketTools[name]:
			sensitive++
		}
	}
	if sensitive == 0 {
		t.Fatalf("the fixture never called a sensitive tool: %v", calls)
	}
	// Each gated call is attempted twice (rejected without a ticket, then retried
	// with one), so the attempts are exactly twice the tickets. A cached ticket
	// would have produced a second rejection instead of a success.
	if confirms == 0 || sensitive != confirms*2 {
		t.Fatalf("want one ticket per retried call, got %d tickets for %d calls: %v",
			confirms, sensitive, calls)
	}
	// Every ticket must sit between the rejected attempt and its retry.
	for index, name := range calls {
		if name != confirmToolName {
			continue
		}
		if index == 0 || index+1 >= len(calls) {
			t.Fatalf("ticket at %d has no surrounding call: %v", index, calls)
		}
		if !ticketTools[calls[index-1]] || calls[index+1] != calls[index-1] {
			t.Fatalf("ticket at %d is not tied to one retried call: %v", index, calls)
		}
	}
}

func TestSyncSurfacesARefusedConfirmationTicket(t *testing.T) {
	// A gateway that will not issue tickets must produce an actionable error, not
	// an empty sync or a bare JSON-RPC code.
	report := oneReport()
	gw := newFakeReportGateway(t, report)
	gw.requireTicket = true
	gw.handlers[confirmToolName] = func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{
			"code": "403", "message": "当前用户无二次确认权限", "data": nil,
		}), false
	}

	_, _, err := NewConnector().FetchIncremental(context.Background(), newTestConfig(gw, nil), nil)
	if err == nil {
		t.Fatal("a refused ticket must fail the sync")
	}
	if !strings.Contains(err.Error(), "二次确认") {
		t.Fatalf("err = %v", err)
	}
}

func TestSyncProjectsTheSelectedFields(t *testing.T) {
	report := oneReport()
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{
		// Deliberately out of report order: the synced columns keep the report's
		// own order so the document reads like the report.
		"fields": []interface{}{"formmain_1_0_field3", "formmain_1_0_field1"},
	})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	overview, ok := handler.itemByID(report.ID)
	if !ok {
		t.Fatal("no overview")
	}
	if overview.Metadata["report_fields"] != "2" || overview.Metadata["report_fields_total"] != "3" {
		t.Fatalf("metadata = %v", overview.Metadata)
	}
	if !strings.Contains(string(overview.Content), "同步字段：2 / 3 列") {
		t.Fatalf("the overview must say how many columns survived:\n%s", overview.Content)
	}

	part, ok := handler.itemByID(overview.SubtreeKeep[0])
	if !ok {
		t.Fatal("no part")
	}
	content := string(part.Content)
	if !strings.Contains(content, "| 单据号 | 金额 |") {
		t.Fatalf("the part header must list only the selected columns:\n%s", content)
	}
	if strings.Contains(content, "部门") {
		t.Fatalf("an unselected column leaked into the part:\n%s", content)
	}
	// The values must be projected in the same order, not just the header.
	if !strings.Contains(content, "NO-1") || !strings.Contains(content, "100") {
		t.Fatalf("the selected values are missing:\n%s", content)
	}
}

func TestSyncFallsBackToEveryColumnWhenNoSelectedFieldExists(t *testing.T) {
	// The report can change under a saved selection. Producing a zero-column
	// document would silently lose the data, so the sync falls back to everything
	// and warns instead.
	report := oneReport()
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{
		"fields": []interface{}{"formmain_1_0_field999"},
	})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	overview, ok := handler.itemByID(report.ID)
	if !ok {
		t.Fatal("no overview")
	}
	if overview.Metadata["report_fields"] != "3" {
		t.Fatalf("the sync must fall back to every column, metadata = %v", overview.Metadata)
	}
	part, _ := handler.itemByID(overview.SubtreeKeep[0])
	if !strings.Contains(string(part.Content), "部门") {
		t.Fatalf("the fallback must keep the unselected column:\n%s", part.Content)
	}
}

func TestSyncKeepsTheKnownFieldsWhenOnlySomeAreMissing(t *testing.T) {
	report := oneReport()
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{
		"fields": []interface{}{"formmain_1_0_field2", "gone_field"},
	})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	overview, _ := handler.itemByID(report.ID)
	if overview.Metadata["report_fields"] != "1" {
		t.Fatalf("only the known field must be kept, metadata = %v", overview.Metadata)
	}
	part, _ := handler.itemByID(overview.SubtreeKeep[0])
	if !strings.Contains(string(part.Content), "部门") {
		t.Fatalf("the surviving field is missing:\n%s", part.Content)
	}
}

func TestSyncSendsTheConfiguredConditions(t *testing.T) {
	report := oneReport()
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	ds := newTestConfig(gw, map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"tableName": "formmain_1_0", "fieldName": "field2",
				"op": "equal", "value": "研发",
			},
		},
	})
	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	if len(calls) == 0 {
		t.Fatal("the data tool was never called")
	}
	// The recorded arguments are the JSON-decoded wire form the gateway sees.
	conditions, ok := calls[0]["conditions"].([]interface{})
	if !ok || len(conditions) != 1 {
		t.Fatalf("conditions sent = %v", calls[0]["conditions"])
	}
	condition, ok := conditions[0].(map[string]interface{})
	if !ok {
		t.Fatalf("condition = %v", conditions[0])
	}
	// The operator is canonicalized to the runtime's own spelling.
	if condition["op"] != "Equal" {
		t.Fatalf("op = %v, want Equal", condition["op"])
	}
	if condition["tableName"] != "formmain_1_0" || condition["fieldName"] != "field2" {
		t.Fatalf("condition = %v", condition)
	}
	if condition["value"] != "研发" {
		t.Fatalf("value = %v", condition["value"])
	}

	overview, _ := handler.itemByID(report.ID)
	if overview.Metadata["report_conditions"] != "1" {
		t.Fatalf("metadata = %v", overview.Metadata)
	}
	if !strings.Contains(string(overview.Content), "固定筛选条件：已生效") {
		t.Fatalf("the overview must disclose that a filter narrowed the data:\n%s", overview.Content)
	}
}

func TestSyncOmitsConditionsWhenNoneAreConfigured(t *testing.T) {
	report := oneReport()
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), newTestConfig(gw, nil), nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if _, ok := calls[0]["conditions"]; ok {
		t.Fatalf("an empty filter must not be sent: %v", calls[0]["conditions"])
	}
	overview, _ := handler.itemByID(report.ID)
	if strings.Contains(string(overview.Content), "固定筛选条件") {
		t.Fatalf("the overview must not claim a filter:\n%s", overview.Content)
	}
}

func TestFetchRefusesMoreThanOneReport(t *testing.T) {
	// The data source is named after its report, so it can only carry one.
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, nil)
	ds.ResourceIDs = []string{query.ID, stats.ID}

	_, _, err := NewConnector().FetchIncremental(context.Background(), ds, nil)
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if !strings.Contains(err.Error(), "exactly one report") {
		t.Fatalf("err = %v", err)
	}
}

func TestFetchTreatsADuplicateSelectionAsOneReport(t *testing.T) {
	// The same id repeated is still one report, so it must not trip the
	// single-report guard.
	report := oneReport()
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, nil)
	ds.ResourceIDs = []string{report.ID, report.ID}

	if _, _, err := NewConnector().FetchIncremental(context.Background(), ds, nil); err != nil {
		t.Fatalf("FetchIncremental: %v", err)
	}
}

func TestDeriveFieldIDsFallsBackToPositions(t *testing.T) {
	// Neither columnIds nor drill metadata: the columns must still be
	// addressable, so a positional id is used.
	ids := deriveFieldIDs([]string{"a", "b"}, nil, nil)
	if len(ids) != 2 || ids[0] != "#0" || ids[1] != "#1" {
		t.Fatalf("ids = %v", ids)
	}

	// A drill list that identifies only the leading columns leaves the rest with
	// their position, so the array is never short.
	row := json.RawMessage(`{"drillContext":{"fieldValues":[{"tableName":"t","fieldName":"a"}]}}`)
	ids = deriveFieldIDs([]string{"a", "b"}, nil, []json.RawMessage{row})
	if len(ids) != 2 || ids[0] != "t_a" || ids[1] != "#1" {
		t.Fatalf("ids = %v", ids)
	}
}

// The runtime answers 200 and returns the *unfiltered* rows when it cannot apply
// a condition, listing it under ignoredConditions. Syncing anyway would ingest
// hundreds of thousands of rows where the operator asked for a slice, so the sync must fail loudly.
func TestSyncRefusesToSyncWhenTheGatewayDropsACondition(t *testing.T) {
	report := oneReport()
	report.Filters = []fakeFilterField{{
		TableName: "formmain_1_0", FieldName: "modify_date",
		FieldDisplay: "修改时间_系统字段", InputType: "datetime",
	}}
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"tableName": "formmain_1_0", "fieldName": "field9001",
				"op": "Greater", "value": "2026-10-01 00:00:00",
			},
		},
	})
	handler := &collectHandler{}
	_, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
	// Nothing may be emitted: the first page is rejected before any row renders.
	if len(handler.items) != 0 {
		t.Fatalf("emitted %d items before rejecting the dropped condition", len(handler.items))
	}
	for _, want := range []string{"field9001", "FIELD_NOT_FOUND"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

// A CAP4 system field is filterable although it is never a display column, so
// the guard must accept it: this is the field a time-window sync filters on.
func TestSyncAcceptsAConditionOnASystemField(t *testing.T) {
	report := oneReport()
	report.Filters = []fakeFilterField{{
		TableName: "formmain_1_0", FieldName: "modify_date",
		FieldDisplay: "修改时间_系统字段", InputType: "datetime",
	}}
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	ds := newTestConfig(gw, map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"tableName": "formmain_1_0", "fieldName": "modify_date",
				"op": "Greater", "value": "2026-10-01 00:00:00",
			},
		},
	})
	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("no documents were emitted")
	}
	conditions, _ := calls[0]["conditions"].([]interface{})
	if len(conditions) != 1 {
		t.Fatalf("conditions sent = %v", calls[0]["conditions"])
	}
}

// probeReport is oneReport plus the CAP4 system fields a probe needs: a
// filterable modify_date marker and a displayed date column to order by.
func probeReport(rows int) *fakeReport {
	report := oneReport()
	body := make([][]string, 0, rows)
	for i := 0; i < rows; i++ {
		body = append(body, []string{fmt.Sprintf("NO-%03d", i), "研发", "100"})
	}
	report.Rows = body
	report.Filters = []fakeFilterField{
		{TableName: "formmain_1_0", FieldName: "modify_date", FieldDisplay: "修改时间_系统字段", InputType: "datetime"},
		{TableName: "formmain_1_0", FieldName: "field1", FieldDisplay: "单据号", InputType: "datetime"},
	}
	return report
}

// seededCursor builds a stored cursor that already carries a completed full read.
func seededCursor(state reportCursor, lastFull time.Time) *types.SyncCursor {
	state.LastFullSync = lastFull
	state.ChangeField = "formmain_1_0.modify_date"
	state.SortField = "formmain_1_0.field1"
	seed := &syncCursor{Reports: map[string]reportCursor{stateKey: state}}
	return seed.encode(time.Now())
}

const stateKey = "1"

// Nothing changed: the probe answers zero modified rows and the same total, so
// the whole large-report walk is skipped and the cursor keeps its watermark.
func TestSyncSkipsTheReadWhenNothingChanged(t *testing.T) {
	report := probeReport(5)
	report.FilterByChangeProbe = true
	report.ProbeChanged = 0
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	ds := newTestConfig(gw, nil)
	seed := seededCursor(reportCursor{
		Total: 5, Watermark: "2026-10-17 09:00:00", Rows: 5,
	}, time.Now().Add(-time.Hour))

	handler := &collectHandler{}
	next, err := NewConnector().FetchStream(context.Background(), ds, seed, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) != 0 {
		t.Fatalf("emitted %d documents for an unchanged report", len(handler.items))
	}
	// Two probe requests (total, changed) and no page walk.
	if len(calls) != 2 {
		t.Fatalf("query calls = %d, want 2 probe calls", len(calls))
	}
	for _, call := range calls {
		if call["pageSize"] != "1" {
			t.Fatalf("pageSize = %v, want 1 for a probe", call["pageSize"])
		}
	}

	state, _, err := decodeCursor(next)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	saved := state.Reports[stateKey]
	if saved.Watermark != "2026-10-17 09:00:00" {
		t.Fatalf("watermark = %q, want it unchanged when nothing was read", saved.Watermark)
	}
}

// A row changed upstream: the probe reports it and the full read runs.
func TestSyncReReadsWhenTheProbeSeesChanges(t *testing.T) {
	report := probeReport(5)
	report.FilterByChangeProbe = true
	report.ProbeChanged = 3
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, nil)
	seed := seededCursor(reportCursor{
		Total: 5, Watermark: "2026-10-17 09:00:00", Rows: 5,
	}, time.Now().Add(-time.Hour))

	handler := &collectHandler{}
	next, err := NewConnector().FetchStream(context.Background(), ds, seed, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("a changed report emitted nothing")
	}
	state, _, err := decodeCursor(next)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if state.Reports[stateKey].Watermark == "2026-10-17 09:00:00" {
		t.Fatal("the watermark did not advance after a complete read")
	}
}

// A row count change is detected without the second probe call.
func TestSyncReReadsWhenTheRowCountChanged(t *testing.T) {
	report := probeReport(6)
	report.FilterByChangeProbe = true
	report.ProbeChanged = 0
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	ds := newTestConfig(gw, nil)
	// The stored total says 5 but the source now has 6.
	seed := seededCursor(reportCursor{
		Total: 5, Watermark: "2026-10-17 09:00:00", Rows: 5,
	}, time.Now().Add(-time.Hour))

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, seed, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("a row-count change emitted nothing")
	}
	// Only the total probe ran: the count already proved something changed.
	if len(calls) != 2 {
		t.Fatalf("query calls = %d, want the total probe plus one page", len(calls))
	}
}

// Past the forced-full window the probe is skipped, so a source that does not
// maintain modify_date still converges.
func TestSyncForcesAFullReadAfterTheResyncWindow(t *testing.T) {
	report := probeReport(5)
	report.FilterByChangeProbe = true
	report.ProbeChanged = 0
	gw := newFakeReportGateway(t, report)

	var calls []map[string]interface{}
	captureArgs(gw, toolIDQuery, &calls)

	ds := newTestConfig(gw, map[string]interface{}{"full_resync_hours": float64(1)})
	seed := seededCursor(reportCursor{
		Total: 5, Watermark: "2026-10-17 09:00:00", Rows: 5,
	}, time.Now().Add(-48*time.Hour))

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, seed, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("the forced full read emitted nothing")
	}
	for _, call := range calls {
		if call["pageSize"] == "1" {
			t.Fatal("a probe ran although the forced-full window had expired")
		}
	}
}

// The request order is ascending while the report fits, and newest-first once
// max_rows truncates it (a truncated snapshot should keep the recent rows).
func TestSnapshotOrderAdaptsToTruncation(t *testing.T) {
	c := &Connector{cfg: &config{}}
	if got := c.snapshotOrder(toolRoleStatistics, "formmain_1_0.field1", false); got != nil {
		t.Fatalf("statistics order = %v, want none", got)
	}
	if got := c.snapshotOrder(toolRoleQuery, "", false); got != nil {
		t.Fatalf("order without a key = %v, want none", got)
	}
	asc := c.snapshotOrder(toolRoleQuery, "formmain_1_0.field1", false)
	if len(asc) != 1 || asc[0].Order != "asc" || asc[0].FieldName != "field1" {
		t.Fatalf("order = %+v, want one ascending sort on field1", asc)
	}
	desc := c.snapshotOrder(toolRoleQuery, "formmain_1_0.field1", true)
	if len(desc) != 1 || desc[0].Order != "desc" {
		t.Fatalf("truncating order = %+v, want descending", desc)
	}
}

// Per-row granularity emits one self-describing document per row, keyed by the
// runtime row id so inserts do not renumber the report.
func TestRowGranularityEmitsOneDocumentPerRow(t *testing.T) {
	report := probeReport(3)
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, map[string]interface{}{"document_granularity": "row"})
	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	var rows []types.FetchedItem
	for _, item := range handler.items {
		if strings.Contains(item.ExternalID, "#row#") {
			rows = append(rows, item)
		}
	}
	if len(rows) != 3 {
		t.Fatalf("row documents = %d, want 3", len(rows))
	}
	// The row id from the runtime is part of the external id.
	if !strings.Contains(rows[0].ExternalID, "1-1") {
		t.Fatalf("external id = %q, want it keyed by the runtime row id", rows[0].ExternalID)
	}
	body := string(rows[0].Content)
	for _, want := range []string{"单据号：", "部门：研发"} {
		if !strings.Contains(body, want) {
			t.Fatalf("row document does not contain %q:\n%s", want, body)
		}
	}
}

// Per-row granularity is refused for a report whose keep list would hold
// hundreds of thousands of ids.
func TestRowGranularityRefusesAHugeReport(t *testing.T) {
	report := probeReport(3)
	report.Rows = make([][]string, maxRowDocuments+1)
	for i := range report.Rows {
		report.Rows[i] = []string{"x", "y", "z"}
	}
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, map[string]interface{}{"document_granularity": "row"})
	_, err := NewConnector().FetchStream(context.Background(), ds, nil, &collectHandler{})
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "document_granularity=part") {
		t.Fatalf("error %q does not point at the way out", err)
	}
}

// Every document of a report lives in a folder named after the report, derived
// from the relative file name the sync path splits.
func TestDocumentsLandInAFolderNamedAfterTheReport(t *testing.T) {
	report := probeReport(2)
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, nil)
	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) == 0 {
		t.Fatal("no documents emitted")
	}
	for _, item := range handler.items {
		if !strings.HasPrefix(item.FileName, report.Name+"/") {
			t.Fatalf("file name = %q, want it inside the %q folder", item.FileName, report.Name)
		}
	}
}

// The watermark is rendered in the gateway's timezone, not the sync host's.
func TestWatermarkUsesTheConfiguredTimezone(t *testing.T) {
	at := time.Date(2026, 10, 17, 1, 30, 0, 0, time.UTC)

	shanghai := &Connector{cfg: &config{}}
	// A zero-value config still resolves the deployment default (Asia/Shanghai,
	// UTC+8), so 01:30Z is 09:30 there.
	if got := shanghai.watermark(at); got != "2026-10-17 09:30:00" {
		t.Fatalf("default watermark = %q, want 2026-10-17 09:30:00", got)
	}

	utc := &Connector{cfg: &config{TimestampLocation: time.UTC}}
	if got := utc.watermark(at); got != "2026-10-17 01:30:00" {
		t.Fatalf("utc watermark = %q", got)
	}

	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("LoadLocation: %v", err)
	}
	ny := &Connector{cfg: &config{TimestampLocation: newYork}}
	// 01:30Z is 21:30 the previous day in New York (EDT, UTC-4).
	if got := ny.watermark(at); got != "2026-10-16 21:30:00" {
		t.Fatalf("new york watermark = %q", got)
	}
}

// An unknown timezone must fail at configuration time, not on every sync.
func TestUnknownTimezoneIsAConfigurationError(t *testing.T) {
	_, err := parseConfig(&types.DataSourceConfig{
		Credentials: map[string]interface{}{
			"base_url":     testGatewayURL,
			"access_token": "t",
		},
		Settings: map[string]interface{}{"timestamp_timezone": "Asia/Atlantis"},
	})
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("err = %v, want ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "Asia/Shanghai") {
		t.Fatalf("error %q does not name a valid example", err)
	}
}

// The overview must stay small even when every row is its own document.
func TestOverviewCapsThePartInventory(t *testing.T) {
	report := probeReport(maxOverviewPartRows + 20)
	gw := newFakeReportGateway(t, report)

	ds := newTestConfig(gw, map[string]interface{}{"document_granularity": "row"})
	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	var overview string
	for _, item := range handler.items {
		if item.ExternalID == report.ID {
			overview = string(item.Content)
		}
	}
	if overview == "" {
		t.Fatal("no overview document was emitted")
	}
	if !strings.Contains(overview, "其余 20 个分片未列出") {
		t.Fatalf("overview does not disclose the truncated inventory:\n%s", overview)
	}
	if got := strings.Count(overview, "| 第 "); got != maxOverviewPartRows {
		t.Fatalf("listed %d parts, want %d", got, maxOverviewPartRows)
	}
}

// The report picker asks for matches only: the whole listing is well over a thousand reports
// (and a bare designType listing has already timed out the gateway at 504).
func TestSearchResourcesReturnsOnlyTheMatches(t *testing.T) {
	gw := newFakeReportGateway(t,
		&fakeReport{ID: "1", Name: "示例查询报表", DesignType: designTypeQuery},
		&fakeReport{ID: "2", Name: "销售订单统计", DesignType: designTypeQuery},
	)
	ds := &types.DataSourceConfig{
		Type:        "seeyon",
		Credentials: map[string]interface{}{"base_url": gw.url(), "access_token": testToken},
	}

	found, err := (&Connector{}).SearchResources(context.Background(), ds, "示例查询")
	if err != nil {
		t.Fatalf("SearchResources: %v", err)
	}
	if len(found) != 1 || found[0].ExternalID != "1" {
		t.Fatalf("search returned %+v, want only report 1", found)
	}
	if found[0].Type != "report" || found[0].Name != "示例查询报表" {
		t.Fatalf("resource not shaped like a report: %+v", found[0])
	}
}

func TestSearchResourcesRefusesToAnswerWithEverything(t *testing.T) {
	// An empty keyword must not fall back to the full listing — that is the very
	// behaviour this method exists to remove. It must also not touch the gateway,
	// so a nil config is safe here.
	for _, keyword := range []string{"", "   ", "\t\n"} {
		found, err := (&Connector{}).SearchResources(context.Background(), nil, keyword)
		if err != nil {
			t.Fatalf("SearchResources(%q): %v", keyword, err)
		}
		if found == nil {
			t.Fatalf("SearchResources(%q) must return an empty slice, not nil", keyword)
		}
		if len(found) != 0 {
			t.Fatalf("SearchResources(%q) returned %d resources, want none", keyword, len(found))
		}
	}
}
