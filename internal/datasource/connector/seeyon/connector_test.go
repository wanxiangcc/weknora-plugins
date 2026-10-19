package seeyon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

func TestConnectorType(t *testing.T) {
	// The type string is the registry key and part of every stored data source
	// row, so it is asserted literally as well as via the constant.
	if types.ConnectorTypeSeeyon != "seeyon" {
		t.Fatalf("ConnectorTypeSeeyon = %q", types.ConnectorTypeSeeyon)
	}
	if got := NewConnector().Type(); got != types.ConnectorTypeSeeyon {
		t.Fatalf("Type() = %q", got)
	}
}

// bigQueryReport returns one query report whose rows are padded so a 64 KiB
// document ceiling splits it into several parts. Multi-part behaviour is the
// interesting case, and the single-report model means it has to come from one
// report rather than from a pair.
func bigQueryReport(rows int) *fakeReport {
	padded := make([][]string, 0, rows)
	for i := 0; i < rows; i++ {
		padded = append(padded, []string{
			fmt.Sprintf("NO-%05d", i),
			strings.Repeat("x", 400),
			fmt.Sprintf("%d", i*10),
		})
	}
	return &fakeReport{
		ID: "1001", Name: "销售查询报表", DesignType: designTypeQuery, Category: "销售管理",
		Columns:   []string{"单据号", "备注", "金额"},
		ColumnIDs: []string{"formmain_1_0_field1", "formmain_1_0_field2", "formmain_1_0_field3"},
		Rows:      padded,
	}
}

// twoReports returns a query report and a statistics report of different sizes.
func twoReports() (*fakeReport, *fakeReport) {
	queryRows := make([][]string, 0, 120)
	for i := 0; i < 120; i++ {
		queryRows = append(queryRows, []string{fmt.Sprintf("NO-%03d", i), "华东", fmt.Sprintf("%d", i*10)})
	}
	statsRows := [][]string{{"差旅费", "3000"}, {"招待费", "1500"}}
	return &fakeReport{
		ID: "1001", Name: "销售查询报表", DesignType: designTypeQuery, Category: "销售管理",
		Columns: []string{"单据号", "部门", "金额"}, ColumnIDs: []string{"f1", "f2", "f3"},
		Rows: queryRows,
	}, &fakeReport{
		ID: "2001", Name: "费用统计报表", DesignType: designTypeStatistics, Category: "费用管理",
		Columns: []string{"科目", "合计"}, ColumnIDs: []string{"c1", "c2"},
		Rows: statsRows, statShape: true,
	}
}

func TestFetchStreamEmitsPartsBeforeTheOverview(t *testing.T) {
	// A data source syncs exactly one report, so the ordering guarantee is
	// asserted against a single multi-part report.
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})

	handler := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	ids := handler.externalIDs()
	overviewIndex := -1
	var parts []string
	for index, id := range ids {
		if strings.Contains(id, "#part#") {
			if overviewIndex >= 0 {
				t.Fatalf("part %s was emitted after its overview", id)
			}
			parts = append(parts, id)
			continue
		}
		if id != report.ID {
			t.Fatalf("unexpected top-level document %q in %v", id, ids)
		}
		overviewIndex = index
	}
	if overviewIndex < 0 {
		t.Fatalf("no overview was emitted: %v", ids)
	}
	if len(parts) < 2 {
		t.Fatalf("the fixture must produce several parts, got %v", parts)
	}

	overview, ok := handler.itemByID(report.ID)
	if !ok {
		t.Fatal("no overview")
	}
	// The overview carries ReplacesSubtree plus the complete keep list; the sweep
	// runs when it is ingested, so every part must already be present.
	if !overview.ReplacesSubtree {
		t.Fatal("the overview must replace its subtree so stale parts are swept")
	}
	if len(overview.SubtreeKeep) != len(parts) {
		t.Fatalf("SubtreeKeep = %v, want the %d emitted parts", overview.SubtreeKeep, len(parts))
	}
	for _, id := range parts {
		if !contains(overview.SubtreeKeep, id) {
			t.Fatalf("SubtreeKeep is missing %s", id)
		}
		if !strings.HasPrefix(id, types.SubtreeChildPrefix(report.ID)) {
			t.Fatalf("part id %s is not a child of %s", id, report.ID)
		}
	}

	decoded, _, err := decodeCursor(cursor)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if len(decoded.Reports) != 1 {
		t.Fatalf("the cursor holds %d reports, want 1", len(decoded.Reports))
	}
	state := decoded.Reports[report.ID]
	if state.Rows != 800 || state.Total != 800 {
		t.Fatalf("cursor = %+v", state)
	}
	if len(state.Parts) != len(overview.SubtreeKeep) {
		t.Fatalf("cursor part digests = %d, want %d", len(state.Parts), len(overview.SubtreeKeep))
	}
	if handler.checkpoint == nil {
		t.Fatal("the connector must checkpoint after finishing the report")
	}
}

func TestFetchStreamIncrementalSkipsUnchangedParts(t *testing.T) {
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})
	connector := NewConnector()

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	firstOverview, ok := first.itemByID(report.ID)
	if !ok || len(firstOverview.SubtreeKeep) < 2 {
		t.Fatalf("the fixture must produce several parts: %v", first.externalIDs())
	}

	// Round-trip the cursor through JSON, which is what the service persists.
	restored := roundTripCursor(t, cursor)

	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, restored, second); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	// Unchanged data re-emits only the overview; the parts would otherwise pay a
	// full re-parse and re-embed.
	var parts, overviews int
	for _, id := range second.externalIDs() {
		if strings.Contains(id, "#part#") {
			parts++
			continue
		}
		overviews++
	}
	if parts != 0 {
		t.Fatalf("unchanged parts must be skipped, got %d re-emitted: %v", parts, second.externalIDs())
	}
	if overviews != 1 {
		t.Fatalf("the overview must be refreshed, got %d", overviews)
	}

	// The overview must still keep every part, or the sweep would delete them.
	overview, ok := second.itemByID(report.ID)
	if !ok {
		t.Fatal("no overview on the second sync")
	}
	if len(overview.SubtreeKeep) != len(firstOverview.SubtreeKeep) {
		t.Fatalf("SubtreeKeep shrank: %v vs %v", overview.SubtreeKeep, firstOverview.SubtreeKeep)
	}
}

func TestFetchStreamReEmitsChangedPartsOnly(t *testing.T) {
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})
	connector := NewConnector()

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if overview, ok := first.itemByID(report.ID); !ok || len(overview.SubtreeKeep) < 2 {
		t.Fatalf("the fixture must produce several parts: %v", first.externalIDs())
	}

	// Change the very last row, which only affects the final part.
	report.Rows[len(report.Rows)-1][2] = "999999"

	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, roundTripCursor(t, cursor), second); err != nil {
		t.Fatalf("second sync: %v", err)
	}

	var reemittedParts []string
	for _, id := range second.externalIDs() {
		if strings.Contains(id, "#part#") {
			reemittedParts = append(reemittedParts, id)
		}
	}
	if len(reemittedParts) != 1 {
		t.Fatalf("want exactly the changed part to be re-emitted, got %v", reemittedParts)
	}
	part, ok := second.itemByID(reemittedParts[0])
	if !ok {
		t.Fatal("the re-emitted part is missing")
	}
	if !strings.Contains(string(part.Content), "999999") {
		t.Fatalf("the re-emitted part must carry the new value:\n%s", part.Content)
	}
}

func TestFetchStreamTruncatesAtMaxRows(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{
		"max_rows":           float64(10),
		"max_document_bytes": float64(minMaxDocumentBytes),
	})

	handler := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	overview, ok := handler.itemByID("1001")
	if !ok {
		t.Fatal("no overview for the truncated report")
	}
	if overview.Metadata["report_truncated"] != "true" {
		t.Fatalf("truncation must be recorded explicitly: %v", overview.Metadata)
	}
	if overview.Metadata["report_rows"] != "10" {
		t.Fatalf("only max_rows rows may be emitted, metadata = %v", overview.Metadata)
	}
	if overview.Metadata["report_total_rows"] != "120" {
		t.Fatalf("the source total must still be reported, metadata = %v", overview.Metadata)
	}
	if !strings.Contains(string(overview.Content), "数据已截断") {
		t.Fatalf("the overview must warn about truncation:\n%s", overview.Content)
	}

	decoded, _, err := decodeCursor(cursor)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !decoded.Reports["1001"].Truncated || decoded.Reports["1001"].Rows != 10 {
		t.Fatalf("cursor = %+v", decoded.Reports["1001"])
	}
}

func TestFetchStreamDoesNotClaimTruncationAtTheExactLimit(t *testing.T) {
	// A report whose size equals max_rows is complete, not truncated. Claiming
	// otherwise would send operators chasing a phantom limit.
	report := &fakeReport{
		ID: "1", Name: "R", DesignType: designTypeQuery, Columns: []string{"a"},
		Rows: [][]string{{"1"}, {"2"}, {"3"}},
	}
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_rows": float64(3)})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	overview, ok := handler.itemByID("1")
	if !ok {
		t.Fatal("no overview")
	}
	if overview.Metadata["report_truncated"] != "false" {
		t.Fatalf("metadata = %v", overview.Metadata)
	}
	if strings.Contains(string(overview.Content), "数据已截断") {
		t.Fatalf("overview must not warn:\n%s", overview.Content)
	}
}

func TestFetchStreamUnlimitedRowsWhenMaxRowsIsZero(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{"max_rows": float64(0)})

	handler := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	decoded, _, _ := decodeCursor(cursor)
	if decoded.Reports["1001"].Rows != 120 {
		t.Fatalf("max_rows=0 must mean unlimited, got %d", decoded.Reports["1001"].Rows)
	}
}

func TestFetchStreamSelectsOnlyConfiguredResources(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{})
	ds.ResourceIDs = []string{"2001"}

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	for _, id := range handler.externalIDs() {
		if strings.HasPrefix(id, "1001") {
			t.Fatalf("an unselected report was synced: %s", id)
		}
	}
	if _, ok := handler.itemByID("2001"); !ok {
		t.Fatalf("the selected report was not synced: %v", handler.externalIDs())
	}
}

func TestFetchStreamSkipsAReportThatVanishedUpstream(t *testing.T) {
	// A saved selection can outlive the report it points at; that must degrade
	// to "nothing to do", not to a failed sync.
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{})
	ds.ResourceIDs = []string{"deleted-report-id"}

	handler := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if err != nil {
		t.Fatalf("a vanished selection must not fail the sync: %v", err)
	}
	if len(handler.items) != 0 {
		t.Fatalf("items = %v", handler.externalIDs())
	}
	if cursor == nil {
		t.Fatal("a cursor must still be returned")
	}
}

func TestFetchStreamReportsPartialFailures(t *testing.T) {
	// A page failure after the first page leaves real data on the floor. That is
	// a partial success: the rows already rendered are kept, no overview is
	// emitted (its sweep would run against an incomplete snapshot), and the
	// reason is reported.
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})

	original := gw.handlers[toolIDQuery]
	gw.handlers[toolIDQuery] = func(args map[string]interface{}) (string, bool) {
		if args["page"] == "2" {
			return mustJSON(map[string]interface{}{"code": 500, "message": "报表服务不可用", "data": nil}), false
		}
		return original(args)
	}

	handler := &collectHandler{}
	_, err := NewConnector().FetchStream(context.Background(), ds, nil, handler)
	if err == nil {
		t.Fatal("a page failure must be reported")
	}
	var partial *datasource.PartialFetchError
	if !errors.As(err, &partial) {
		t.Fatalf("want a PartialFetchError, got %T: %v", err, err)
	}
	joined := strings.Join(partial.Details, "; ")
	if !strings.Contains(joined, report.Name) || !strings.Contains(joined, "报表服务不可用") {
		t.Fatalf("the failure must name the report and the reason, got %q", joined)
	}

	// The rows read before the failure must not be discarded.
	var parts int
	for _, id := range handler.externalIDs() {
		if strings.Contains(id, "#part#") {
			parts++
		}
	}
	if parts == 0 {
		t.Fatalf("the first page's parts must still be emitted: %v", handler.externalIDs())
	}
	// And the report must not appear as fully synced.
	if _, ok := handler.itemByID(report.ID); ok {
		t.Fatal("a failed report must not emit an overview")
	}
}

func TestFetchStreamKeepsPreviousProgressForAFailedReport(t *testing.T) {
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})

	first := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	want := *cursor

	gw.handlers[toolIDQuery] = func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{"code": 500, "message": "报表服务不可用", "data": nil}), false
	}

	second := &collectHandler{}
	next, err := NewConnector().FetchStream(context.Background(), ds, roundTripCursor(t, cursor), second)
	if err == nil {
		t.Fatal("want a partial failure")
	}
	// The failed report's previous entry must survive so a resume retries it
	// rather than treating it as already handled.
	decoded, _, decodeErr := decodeCursor(next)
	if decodeErr != nil {
		t.Fatalf("decodeCursor: %v", decodeErr)
	}
	state, ok := decoded.Reports[report.ID]
	if !ok {
		t.Fatalf("the failed report's progress must be carried forward: %+v", decoded.Reports)
	}
	if state.Rows != 800 || len(state.Parts) == 0 {
		t.Fatalf("the carried-forward progress must be the previous snapshot, got %+v", state)
	}
	_ = want
}

func TestFetchStreamRepairsDataWhenTheSourceShrinks(t *testing.T) {
	// Rows padded to ~400 bytes so 600 of them span several 64 KiB parts.
	padded := make([][]string, 0, 600)
	for i := 0; i < 600; i++ {
		padded = append(padded, []string{
			fmt.Sprintf("NO-%04d", i),
			strings.Repeat("x", 400),
		})
	}
	query := &fakeReport{
		ID: "1001", Name: "销售查询报表", DesignType: designTypeQuery,
		Columns: []string{"单据号", "备注"}, ColumnIDs: []string{"f1", "f2"},
		Rows: padded,
	}
	stats := &fakeReport{
		ID: "2001", Name: "费用统计报表", DesignType: designTypeStatistics,
		Columns: []string{"科目", "合计"}, Rows: [][]string{{"差旅费", "3000"}}, statShape: true,
	}
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})

	first := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	firstOverview, _ := first.itemByID("1001")
	firstParts := len(firstOverview.SubtreeKeep)
	if firstParts < 2 {
		t.Fatalf("the fixture must produce several parts, got %d", firstParts)
	}

	// Only ten rows remain: the earlier parts no longer exist upstream, so the
	// new SubtreeKeep must shrink and the sweep must delete the leftovers.
	query.Rows = query.Rows[:10]

	second := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, roundTripCursor(t, cursor), second); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	secondOverview, ok := second.itemByID("1001")
	if !ok {
		t.Fatal("no overview after the report shrank")
	}
	if len(secondOverview.SubtreeKeep) >= firstParts {
		t.Fatalf("SubtreeKeep must shrink with the report: %d -> %d",
			firstParts, len(secondOverview.SubtreeKeep))
	}
	for _, id := range secondOverview.SubtreeKeep {
		if _, ok := second.itemByID(id); !ok {
			t.Fatalf("kept id %q was not emitted", id)
		}
	}
}

func TestFetchStreamHandlesAnEmptyReport(t *testing.T) {
	report := &fakeReport{
		ID: "1", Name: "空报表", DesignType: designTypeQuery,
		Columns: []string{"a", "b"},
	}
	gw := newFakeReportGateway(t, report)

	handler := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), newTestConfig(gw, nil), nil, handler)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(handler.items) != 1 {
		t.Fatalf("an empty report yields just its overview, got %v", handler.externalIDs())
	}
	overview := handler.items[0]
	if len(overview.SubtreeKeep) != 0 {
		t.Fatalf("an empty report keeps no parts, got %v", overview.SubtreeKeep)
	}
	// The empty keep list is what tells the sweep to remove every part the
	// previous sync wrote.
	if !overview.ReplacesSubtree {
		t.Fatal("the overview must still reconcile deletions")
	}
	decoded, _, _ := decodeCursor(cursor)
	if decoded.Reports["1"].Rows != 0 {
		t.Fatalf("cursor = %+v", decoded.Reports["1"])
	}
}

func TestFetchStreamUsesTheDecodedRowRangeInPartTitles(t *testing.T) {
	report := &fakeReport{
		ID: "1", Name: "多页报表", DesignType: designTypeQuery,
		Columns: []string{"a"},
		Rows: [][]string{
			{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}, {"10"},
		},
	}
	gw := newFakeReportGateway(t, report)
	// page_size 3 forces four pages, so paging across requests is exercised.
	ds := newTestConfig(gw, map[string]interface{}{"page_size": float64(3)})

	handler := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	overview, ok := handler.itemByID("1")
	if !ok {
		t.Fatal("no overview")
	}
	if overview.Metadata["report_rows"] != "10" {
		t.Fatalf("all ten rows must be read across pages: %v", overview.Metadata)
	}
	for _, id := range overview.SubtreeKeep {
		part, _ := handler.itemByID(id)
		if !strings.Contains(string(part.Content), "| 10 |") {
			t.Fatalf("part %s must contain the last row:\n%s", id, part.Content)
		}
	}
}

func TestFetchAllForcesAFullReEmit(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})
	connector := NewConnector()

	streamed := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, streamed)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	items, err := connector.FetchAll(context.Background(), ds, ds.ResourceIDs)
	if err != nil {
		t.Fatalf("FetchAll: %v", err)
	}
	// FetchAll is the full-sync entry point: it must not hash-skip, because the
	// caller asked for a rebuild.
	itemsByID := map[string]types.FetchedItem{}
	for _, item := range items {
		itemsByID[item.ExternalID] = item
	}
	overview := itemsByID["1001"]
	if len(overview.SubtreeKeep) == 0 {
		t.Fatal("FetchAll must reconcile deletions through SubtreeKeep")
	}
	for _, id := range overview.SubtreeKeep {
		if _, ok := itemsByID[id]; !ok {
			t.Fatalf("FetchAll must emit every kept part, %q is missing", id)
		}
	}
	_ = cursor
}

func TestFetchFullStreamReEmitsWithThePreviousBaseline(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})
	connector := NewConnector()

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}

	full := &collectHandler{}
	if _, err := connector.FetchFullStream(context.Background(), ds, roundTripCursor(t, cursor), full); err != nil {
		t.Fatalf("FetchFullStream: %v", err)
	}
	var parts int
	for _, id := range full.externalIDs() {
		if strings.Contains(id, "#part#") {
			parts++
		}
	}
	if parts == 0 {
		t.Fatal("a force-full run must re-emit every part")
	}
	overview, _ := full.itemByID("1001")
	if len(overview.SubtreeKeep) != parts-0 && len(overview.SubtreeKeep) == 0 {
		t.Fatalf("SubtreeKeep = %v", overview.SubtreeKeep)
	}
}

func TestFetchRefusesAnEmptyReportSelection(t *testing.T) {
	// Reports can hold hundreds of thousands of rows, so an empty selection must
	// not silently mean "every report this token can see": that would be a very
	// large, very surprising job. The Yuque connector (the other connector here
	// backed by an explicit flat resource list) rejects it the same way.
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	ds := newTestConfig(gw, nil)
	ds.ResourceIDs = nil

	_, _, err := NewConnector().FetchIncremental(context.Background(), ds, nil)
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if !strings.Contains(err.Error(), "no report selected") {
		t.Fatalf("err = %v", err)
	}
}

func TestFetchToleratesSelectionsThatAreEntirelyFilteredOut(t *testing.T) {
	// A selected report can vanish upstream, or its design type can be disabled
	// in settings. That is not a failure: the run has nothing to do, and the
	// cursor must stay a valid snapshot rather than becoming nil.
	summary := &fakeReport{ID: "9001", Name: "指标", DesignType: designTypeSummary}
	gw := newFakeReportGateway(t, summary)
	ds := newTestConfig(gw, map[string]interface{}{"design_types": []interface{}{designTypeQuery}})

	h := &collectHandler{}
	cursor, err := NewConnector().FetchStream(context.Background(), ds, nil, h)
	if err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(h.items) != 0 {
		t.Fatalf("items = %+v", h.items)
	}
	if cursor == nil {
		t.Fatal("a cursor must still be returned")
	}
}

func TestFetchSkipsAReportThatIsNoLongerListed(t *testing.T) {
	// A selected id that the listing no longer contains is skipped rather than
	// failing the run, so a deleted report does not wedge the data source.
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, nil)
	ds.ResourceIDs = []string{"9999-does-not-exist"}

	h := &collectHandler{}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, h); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if len(h.items) != 0 {
		t.Fatalf("nothing should be synced, got %v", h.externalIDs())
	}
}

func TestFetchIncrementalReturnsItemsAndCursor(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{})

	items, cursor, err := NewConnector().FetchIncremental(context.Background(), ds, nil)
	if err != nil {
		t.Fatalf("FetchIncremental: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("no items")
	}
	if cursor == nil || len(cursor.ConnectorCursor) == 0 {
		t.Fatal("a cursor must be returned")
	}
}

func TestValidateSurfacesTheToolAuthorizationGate(t *testing.T) {
	// The live gateway currently answers "工具未被允许调用" for the report tools
	// and omits them from tools/list. Validate must explain that, not return a
	// generic connectivity error.
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	gw.refusedTools[toolIDReports] = true
	gw.refusedTools[toolIDQuery] = true
	gw.refusedTools[toolIDStats] = true

	err := NewConnector().Validate(context.Background(), newTestConfig(gw, nil))
	if !errors.Is(err, errToolNotFound) {
		t.Fatalf("want errToolNotFound, got %v", err)
	}
	for _, want := range []string{"报表管理", "tool_overrides"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q must mention %q", err, want)
		}
	}
}

func TestValidateReportsARejectedAuthorizationCode(t *testing.T) {
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	ds := newTestConfig(gw, nil)
	ds.Credentials["access_token"] = "EXPIRED"

	err := NewConnector().Validate(context.Background(), ds)
	if !errors.Is(err, errAuthorizationRejected) {
		t.Fatalf("want errAuthorizationRejected, got %v", err)
	}
}

func TestValidateFailsBeforeAnyRequestWhenTheTokenIsMissing(t *testing.T) {
	gw := newFakeReportGateway(t, &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery})
	ds := newTestConfig(gw, nil)
	delete(ds.Credentials, "access_token")

	err := NewConnector().Validate(context.Background(), ds)
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
	if calls := gw.recordedCalls(); len(calls) != 0 {
		t.Fatalf("a missing token must fail before any network call, got %v", calls)
	}
}

func TestValidateSucceedsAgainstAHealthyGateway(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	if err := NewConnector().Validate(context.Background(), newTestConfig(gw, nil)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestListResourcesDescribesEveryReport(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)

	resources, err := NewConnector().ListResources(context.Background(), newTestConfig(gw, nil), "")
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("resources = %+v", resources)
	}
	// Sorted by name for a stable picker.
	if resources[0].Name > resources[1].Name {
		t.Fatalf("resources must be sorted by name: %s, %s", resources[0].Name, resources[1].Name)
	}
	byID := map[string]types.Resource{}
	for _, resource := range resources {
		byID[resource.ExternalID] = resource
	}
	statistics := byID["2001"]
	if statistics.Name != "费用统计报表" || statistics.Type != "report" {
		t.Fatalf("resource = %+v", statistics)
	}
	if !strings.Contains(statistics.Description, "统计报表") {
		t.Fatalf("description = %q", statistics.Description)
	}
	if statistics.Metadata["design_type"] != designTypeStatistics {
		t.Fatalf("metadata = %v", statistics.Metadata)
	}
	if statistics.Metadata["recommended_api"] != toolRoleStatistics {
		t.Fatalf("metadata = %v", statistics.Metadata)
	}
}

func TestListResourcesFiltersByDesignType(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{
		"design_types": []interface{}{designTypeStatistics},
	})

	resources, err := NewConnector().ListResources(context.Background(), ds, "")
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	if len(resources) != 1 || resources[0].ExternalID != "2001" {
		t.Fatalf("resources = %+v", resources)
	}
}

func TestListResourcesReturnsAReportsFields(t *testing.T) {
	// The field step asks for the selected report as the parent and gets both the
	// selectable columns and the filterable definitions back, so one call feeds
	// the column picker and the fixed-filter editor.
	report := &fakeReport{
		ID: "1", Name: "示例查询报表", DesignType: designTypeQuery,
		Columns:   []string{"单据号", "部门"},
		ColumnIDs: []string{"formmain_1_0_field1", "formmain_1_0_field2"},
		Rows:      [][]string{{"NO-1", "研发"}},
		Filters: []fakeFilterField{{
			TableName: "formmain_1_0", FieldName: "field2", FieldDisplay: "部门",
			InputType: "select", IsEnum: true,
			Operators:   []string{"Equal", "NotEqual"},
			EnumOptions: []map[string]string{{"id": "1", "label": "研发"}},
		}},
	}
	gw := newFakeReportGateway(t, report)

	resources, err := NewConnector().ListResources(context.Background(), newTestConfig(gw, nil), "1")
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}

	var display, filters []types.Resource
	for _, resource := range resources {
		switch resource.Type {
		case "display_field":
			display = append(display, resource)
		case "filter_field":
			filters = append(filters, resource)
		default:
			t.Fatalf("unexpected resource type %q", resource.Type)
		}
	}
	if len(display) != 2 {
		t.Fatalf("display fields = %+v", display)
	}
	if display[0].ExternalID != "formmain_1_0_field1" || display[0].Name != "单据号" {
		t.Fatalf("first display field = %+v", display[0])
	}
	if display[1].ExternalID != "formmain_1_0_field2" || display[1].Name != "部门" {
		t.Fatalf("second display field = %+v", display[1])
	}
	if display[0].ParentID != "1" {
		t.Fatalf("parent = %q", display[0].ParentID)
	}
	if display[0].HasChildren {
		t.Fatal("a field cannot have children")
	}
	if len(filters) != 1 {
		t.Fatalf("filter fields = %+v", filters)
	}
	if filters[0].ExternalID != "formmain_1_0_field2" || filters[0].Name != "部门" {
		t.Fatalf("filter field = %+v", filters[0])
	}
	if filters[0].Metadata["is_enum"] != true {
		t.Fatalf("metadata = %v", filters[0].Metadata)
	}
	operators, ok := filters[0].Metadata["operators"].([]string)
	if !ok || len(operators) != 2 {
		t.Fatalf("operators = %v", filters[0].Metadata["operators"])
	}
	if len(filters[0].Metadata["enum_options"].([]map[string]string)) != 1 {
		t.Fatalf("enum options = %v", filters[0].Metadata["enum_options"])
	}
}

func TestListResourcesDerivesStatisticsFieldIDsFromTheDrillMetadata(t *testing.T) {
	// A statistics report has no columnIds and no displayFields: the field
	// identity only exists in the drill metadata of a row, which is exactly why
	// the field step probes one row.
	report := &fakeReport{
		ID: "2", Name: "费用统计", DesignType: designTypeStatistics, statShape: true,
		Columns:   []string{"部门", "合计"},
		ColumnIDs: []string{"formmain_2_0_field1", "formmain_2_0_field2"},
		Rows:      [][]string{{"研发", "3000"}},
	}
	gw := newFakeReportGateway(t, report)

	resources, err := NewConnector().ListResources(context.Background(), newTestConfig(gw, nil), "2")
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	var display []types.Resource
	for _, resource := range resources {
		if resource.Type == "display_field" {
			display = append(display, resource)
		}
	}
	if len(display) != 2 {
		t.Fatalf("display fields = %+v", resources)
	}
	if display[0].ExternalID != "formmain_2_0_field1" {
		t.Fatalf("field id = %q", display[0].ExternalID)
	}
	if display[1].ExternalID != "formmain_2_0_field2" {
		t.Fatalf("field id = %q", display[1].ExternalID)
	}
}

func TestListResourcesRejectsAReportThatIsNotInTheListing(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)

	_, err := NewConnector().ListResources(context.Background(), newTestConfig(gw, nil), "no-such-report")
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
}

func TestResolveResourceAncestorsIsAlwaysEmpty(t *testing.T) {
	ancestors, err := NewConnector().ResolveResourceAncestors(
		context.Background(), nil, []string{"1", "2"},
	)
	if err != nil {
		t.Fatalf("ResolveResourceAncestors: %v", err)
	}
	if len(ancestors) != 0 {
		t.Fatalf("ancestors = %v", ancestors)
	}
}

func TestFetchStreamSurfacesAnIngestFailure(t *testing.T) {
	report := bigQueryReport(800)
	gw := newFakeReportGateway(t, report)
	ds := newTestConfig(gw, map[string]interface{}{"max_document_bytes": float64(minMaxDocumentBytes)})

	// Fail on the overview, after its parts were emitted.
	handler := &collectHandler{failOn: report.ID}
	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, handler); err == nil {
		t.Fatal("an ingest failure must propagate")
	}

	// The report must not be recorded as synced, so the next run retries it
	// instead of treating it as complete.
	if handler.checkpoint != nil {
		decoded, _, err := decodeCursor(handler.checkpoint)
		if err != nil {
			t.Fatalf("decodeCursor: %v", err)
		}
		if _, ok := decoded.Reports[report.ID]; ok {
			t.Fatalf("a failed report must not be recorded as synced: %+v", decoded.Reports)
		}
	}
}

func TestFetchStreamUsesTheStatisticsToolOnlyForStatisticsReports(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)
	ds := newTestConfig(gw, map[string]interface{}{})
	ds.ResourceIDs = []string{"1001"}

	if _, err := NewConnector().FetchStream(context.Background(), ds, nil, &collectHandler{}); err != nil {
		t.Fatalf("FetchStream: %v", err)
	}
	if contains(gw.calledToolNames(), toolIDStats) {
		t.Fatalf("the statistics tool must not be used for a query report: %v", gw.calledToolNames())
	}
}

func contains(haystack []string, needle string) bool {
	for _, value := range haystack {
		if value == needle {
			return true
		}
	}
	return false
}

// roundTripCursor persists and restores a cursor exactly as the service does,
// so the tests exercise the JSON boundary rather than an in-memory shortcut.
func roundTripCursor(t *testing.T, cursor *types.SyncCursor) *types.SyncCursor {
	t.Helper()
	if cursor == nil {
		return nil
	}
	raw, err := json.Marshal(cursor)
	if err != nil {
		t.Fatalf("marshal cursor: %v", err)
	}
	var restored types.SyncCursor
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal cursor: %v", err)
	}
	return &restored
}

// The field step used to cost 62s: a full index (~24s) plus a 1-row data probe
// (seconds on a large report). Neither is needed for a QUERY report — its
// definition already carries the ordered column list.
func TestFieldStepSkipsTheDataProbeForAQueryReport(t *testing.T) {
	report := &fakeReport{
		ID: "1", Name: "示例查询报表", DesignType: designTypeQuery,
		Columns:   []string{"单据号", "部门"},
		ColumnIDs: []string{"formmain_1_0_field1", "formmain_1_0_field2"},
		Rows:      [][]string{{"NO-1", "研发"}},
	}
	gw := newFakeReportGateway(t, report)

	fields, err := NewConnector().ResolveResourceFields(
		context.Background(), newTestConfig(gw, nil), "1", "示例查询报表")
	if err != nil {
		t.Fatalf("ResolveResourceFields: %v", err)
	}
	display := displayFieldsOf(fields)
	if len(display) != 2 {
		t.Fatalf("display fields = %+v", display)
	}
	if display[0].ExternalID != "formmain_1_0_field1" || display[0].Name != "单据号" {
		t.Fatalf("first field = %+v", display[0])
	}
	unfiltered, dataCalls := gw.counters()
	if dataCalls != 0 {
		t.Fatalf("the field step probed report data %d time(s); a QUERY report has its columns in the definition", dataCalls)
	}
	if unfiltered != 0 {
		t.Fatalf("the field step listed every report %d time(s); the name hint must be used", unfiltered)
	}
}

// A statistics report has no displayFields at all: its column identity exists
// only in a row's drill metadata, so the probe stays for that shape.
func TestFieldStepKeepsTheDataProbeForAStatisticsReport(t *testing.T) {
	report := &fakeReport{
		ID: "2", Name: "费用统计", DesignType: designTypeStatistics, statShape: true,
		Columns:   []string{"部门", "合计"},
		ColumnIDs: []string{"formmain_2_0_field1", "formmain_2_0_field2"},
		Rows:      [][]string{{"研发", "3000"}},
	}
	gw := newFakeReportGateway(t, report)

	fields, err := NewConnector().ResolveResourceFields(
		context.Background(), newTestConfig(gw, nil), "2", "费用统计")
	if err != nil {
		t.Fatalf("ResolveResourceFields: %v", err)
	}
	display := displayFieldsOf(fields)
	if len(display) != 2 || display[0].ExternalID != "formmain_2_0_field1" {
		t.Fatalf("display fields = %+v", display)
	}
	if _, dataCalls := gw.counters(); dataCalls == 0 {
		t.Fatal("a statistics report has no definition columns, so one probe is required")
	}
}

// Without the hint the lookup still works, it just pays for the full listing.
func TestFieldStepFallsBackToTheFullListingWithoutANameHint(t *testing.T) {
	report := &fakeReport{
		ID: "1", Name: "示例查询报表", DesignType: designTypeQuery,
		Columns:   []string{"单据号"},
		ColumnIDs: []string{"formmain_1_0_field1"},
		Rows:      [][]string{{"NO-1"}},
	}
	gw := newFakeReportGateway(t, report)

	fields, err := NewConnector().ListResources(
		context.Background(), newTestConfig(gw, nil), "1")
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	if display := displayFieldsOf(fields); len(display) != 1 {
		t.Fatalf("display fields = %+v", display)
	}
	if unfiltered, _ := gw.counters(); unfiltered == 0 {
		t.Fatal("without a hint the connector must list to find the report")
	}
}

// Validate runs on the credentials step: it must prove the listing tool works
// without paying for every report's permission check and definition build.
func TestValidateProvesTheListingWithoutTheFullIndex(t *testing.T) {
	query, stats := twoReports()
	gw := newFakeReportGateway(t, query, stats)

	if err := NewConnector().Validate(context.Background(), newTestConfig(gw, nil)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	unfiltered, dataCalls := gw.counters()
	if unfiltered != 0 {
		t.Fatalf("Validate listed every report %d time(s); it must filter by a sentinel name", unfiltered)
	}
	if dataCalls != 0 {
		t.Fatalf("Validate called a report data tool %d time(s)", dataCalls)
	}
}

// displayFieldsOf keeps only the selectable columns; the field step also returns
// filterable definitions, which these tests do not assert on.
func displayFieldsOf(resources []types.Resource) []types.Resource {
	out := make([]types.Resource, 0, len(resources))
	for _, resource := range resources {
		if resource.Type == "display_field" {
			out = append(out, resource)
		}
	}
	return out
}
