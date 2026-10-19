package seeyon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestUnwrapStripsTheOAEnvelope(t *testing.T) {
	payload, err := unwrap("t", json.RawMessage(`{"code":200,"message":"操作成功","data":{"a":1}}`))
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if string(payload) != `{"a":1}` {
		t.Fatalf("payload = %s", payload)
	}

	// A bare payload without "code" is passed through, so the connector keeps
	// working if a tool ever returns the inner object directly.
	payload, err = unwrap("t", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if string(payload) != `{"a":1}` {
		t.Fatalf("payload = %s", payload)
	}
}

func TestUnwrapSurfacesBusinessFailures(t *testing.T) {
	// The report runtime reports a business failure inside the envelope while
	// the JSON-RPC layer still sees success, so this is the only signal.
	_, err := unwrap("t", json.RawMessage(`{"code":500,"message":"报表不存在","data":null}`))
	if err == nil {
		t.Fatal("want an error for a non-200 code")
	}
	if !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "报表不存在") {
		t.Fatalf("error %q must carry the code and the message", err)
	}
}

func TestUnwrapRejectsEmptyAndMalformedPayloads(t *testing.T) {
	cases := []struct {
		name    string
		payload string
	}{
		{name: "empty", payload: `  `},
		{name: "no data", payload: `{"code":200,"message":"操作成功"}`},
		{name: "null data", payload: `{"code":200,"message":"操作成功","data":null}`},
		{name: "malformed json", payload: `{"code":`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := unwrap("t", json.RawMessage(tc.payload)); err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

func TestFlexibleIntAcceptsStringsAndNumbers(t *testing.T) {
	// The CAP4 runtime serializes page, pageSize, totalRecords and totalPages as
	// strings while other builds use numbers, so both must parse.
	var decoded struct {
		A flexibleInt `json:"a"`
		B flexibleInt `json:"b"`
		C flexibleInt `json:"c"`
		D flexibleInt `json:"d"`
	}
	body := `{"a":"123","b":456,"c":"","d":null}`
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.A != 123 || decoded.B != 456 || decoded.C != 0 || decoded.D != 0 {
		t.Fatalf("decoded = %+v", decoded)
	}
	if err := json.Unmarshal([]byte(`{"a":"many"}`), &decoded); err == nil {
		t.Fatal("an unparseable value must be an error")
	}
}

// newFetchClient builds a client pointed at a gateway serving the given reports.
func newFetchClient(t *testing.T, reports ...*fakeReport) (*client, *fakeGateway) {
	t.Helper()
	gw := newFakeReportGateway(t, reports...)
	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	return cli, gw
}

func TestFetchReportPageNormalizesTheQueryRowShape(t *testing.T) {
	// Query reports return data.rows as a flat array of value arrays, with row
	// ids in a parallel data.ids array.
	report := &fakeReport{
		ID: "1001", Name: "销售查询", DesignType: designTypeQuery,
		Columns: []string{"部门", "金额"}, ColumnIDs: []string{"f1", "f2"},
		Rows: [][]string{{"华东", "100"}, {"华南", "200"}},
	}
	cli, _ := newFetchClient(t, report)

	page, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1001", designTypeQuery, 1, 500, false, nil, nil)
	if err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if page.TotalRecords != 2 || page.TotalPages != 1 {
		t.Fatalf("totals = %d/%d, want 2/1", page.TotalRecords, page.TotalPages)
	}
	if !equalRows(page.Rows, [][]string{{"华东", "100"}, {"华南", "200"}}) {
		t.Fatalf("rows = %v", page.Rows)
	}
	if len(page.RowIDs) != 2 || page.RowIDs[0] != "1001-1" {
		t.Fatalf("row ids = %v", page.RowIDs)
	}
	if !equalStrings(page.Columns, []string{"部门", "金额"}) {
		t.Fatalf("columns = %v", page.Columns)
	}
}

func TestFetchReportPageNormalizesTheStatisticsRowShape(t *testing.T) {
	// Statistics reports return data.rows as objects with a "values" array.
	report := &fakeReport{
		ID: "2001", Name: "费用统计", DesignType: designTypeStatistics,
		Columns: []string{"科目", "合计"}, ColumnIDs: []string{"c1", "c2"},
		Rows:      [][]string{{"差旅", "3000"}},
		statShape: true,
	}
	cli, _ := newFetchClient(t, report)

	page, err := cli.fetchReportPage(context.Background(), toolIDStats, "2001", designTypeStatistics, 1, 500, false, nil, nil)
	if err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if !equalRows(page.Rows, [][]string{{"差旅", "3000"}}) {
		t.Fatalf("rows = %v", page.Rows)
	}
}

func TestFetchReportPagePagesThroughTheReport(t *testing.T) {
	rows := make([][]string, 0, 5)
	for i := 0; i < 5; i++ {
		rows = append(rows, []string{"row", string(rune('a' + i))})
	}
	report := &fakeReport{
		ID: "1", Name: "R", DesignType: designTypeQuery,
		Columns: []string{"k", "v"}, Rows: rows,
	}
	cli, _ := newFetchClient(t, report)

	// pageSize 2 yields three pages: 2 + 2 + 1.
	first, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 1, 2, false, nil, nil)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if first.TotalPages != 3 || first.TotalRecords != 5 || len(first.Rows) != 2 {
		t.Fatalf("page 1 = %d rows, %d/%d pages", len(first.Rows), first.TotalRecords, first.TotalPages)
	}
	last, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 3, 2, false, nil, nil)
	if err != nil {
		t.Fatalf("page 3: %v", err)
	}
	if len(last.Rows) != 1 || last.Rows[0][1] != "e" {
		t.Fatalf("page 3 rows = %v", last.Rows)
	}
}

func TestFetchReportPageSendsStringPagination(t *testing.T) {
	// The CAP4 runtime declares page and pageSize as strings; sending numbers
	// can be rejected outright, so the wire format is asserted directly.
	report := &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery, Columns: []string{"k"}}
	gw := newFakeReportGateway(t, report)

	var captured map[string]interface{}
	original := gw.handlers[toolIDQuery]
	gw.handlers[toolIDQuery] = func(args map[string]interface{}) (string, bool) {
		captured = args
		return original(args)
	}

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	if _, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 3, 200, false, nil, nil); err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if captured["page"] != "3" || captured["pageSize"] != "200" {
		t.Fatalf("page/pageSize must be strings, got %#v / %#v", captured["page"], captured["pageSize"])
	}
	if captured["id"] != "1" {
		t.Fatalf("id = %#v", captured["id"])
	}
	if _, ok := captured["designType"]; ok {
		t.Fatalf("the report-runtime query tool must not receive designType: %#v", captured)
	}
}

func TestFetchReportPageSendsDesignTypeOnlyForTheLegacyTool(t *testing.T) {
	report := &fakeReport{ID: "1", Name: "R", DesignType: designTypeStatistics, Columns: []string{"k"}}
	gw := newFakeGateway(t)
	gw.addTool(toolIDReports, descReports, nil)
	gw.addTool(toolIDLegacy, descLegacy, func(args map[string]interface{}) (string, bool) {
		return report.page(args)
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	if cli.legacyTool != toolIDLegacy {
		t.Fatalf("legacyTool = %q", cli.legacyTool)
	}

	var captured map[string]interface{}
	gw.handlers[toolIDLegacy] = func(args map[string]interface{}) (string, bool) {
		captured = args
		return report.page(args)
	}
	if _, err := cli.fetchReportPage(context.Background(), toolIDLegacy, "1", designTypeStatistics, 1, 500, true, nil, nil); err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if captured["designType"] != designTypeStatistics {
		t.Fatalf("the legacy tool needs designType, got %#v", captured)
	}
}

func TestListReportsKeepsOnlyTheSyncableDesignTypes(t *testing.T) {
	// One listing call per design type (the gateway requires designType), and a
	// report whose type is out of scope must never surface as a resource.
	cli, _ := newFetchClient(t,
		&fakeReport{ID: "1", Name: "查询报表", DesignType: designTypeQuery},
		&fakeReport{ID: "2", Name: "统计报表", DesignType: designTypeStatistics, statShape: true},
		&fakeReport{ID: "3", Name: "指标报表", DesignType: designTypeSummary},
	)

	queries, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeQuery})
	if err != nil {
		t.Fatalf("listReports(QUERY): %v", err)
	}
	if len(queries) != 1 || queries[0].ID != "1" {
		t.Fatalf("QUERY listing = %+v", queries)
	}

	stats, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeStatistics})
	if err != nil {
		t.Fatalf("listReports(STATISTICS): %v", err)
	}
	if len(stats) != 1 || stats[0].ID != "2" {
		t.Fatalf("STATISTICS listing = %+v", stats)
	}
	if stats[0].DesignTypeName != "统计报表" {
		t.Fatalf("the gateway's design type name must be preserved, got %q", stats[0].DesignTypeName)
	}

	// SUMMARY is recognised so it can be rejected with a reason, but it is not
	// syncable and must not be offered.
	summary, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeSummary})
	if err != nil {
		t.Fatalf("listReports(SUMMARY): %v", err)
	}
	if len(summary) != 0 {
		t.Fatalf("SUMMARY reports must not be offered as syncable resources, got %+v", summary)
	}
}

func TestListReportsSkipsEntriesWithoutAnID(t *testing.T) {
	// A report with no id cannot be selected or fetched; it must be skipped
	// rather than fail the whole listing.
	gw := newFakeGateway(t)
	gw.addTool(toolIDReports, descReports, func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{
			"code": 200,
			"data": map[string]interface{}{
				"total": 2,
				"reports": []map[string]interface{}{
					{"id": "", "name": "破碎的报表", "designType": designTypeQuery},
					{"id": "9", "name": "正常的报表", "designType": designTypeQuery},
				},
			},
		}), false
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defs, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeQuery})
	if err != nil {
		t.Fatalf("listReports: %v", err)
	}
	if len(defs) != 1 || defs[0].ID != "9" {
		t.Fatalf("defs = %+v", defs)
	}
}

func TestListReportsAcceptsABareArrayPayload(t *testing.T) {
	gw := newFakeGateway(t)
	gw.addTool(toolIDReports, descReports, func(map[string]interface{}) (string, bool) {
		return `[{"id":"7","name":"裸数组","designType":"QUERY"}]`, false
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defs, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeQuery})
	if err != nil {
		t.Fatalf("listReports: %v", err)
	}
	if len(defs) != 1 || defs[0].Name != "裸数组" {
		t.Fatalf("defs = %+v", defs)
	}
}

func TestListReportsTreatsAnAbsentArrayAsAContractViolation(t *testing.T) {
	// A payload shaped like an envelope but without a "reports" field is not
	// something the gateway contract allows. Failing loudly is deliberate: the
	// alternative is an empty resource picker with no explanation.
	gw := newFakeGateway(t)
	gw.addTool(toolIDReports, descReports, func(map[string]interface{}) (string, bool) {
		return `{"total":0}`, false
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	_, err = cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeQuery})
	if err == nil {
		t.Fatal("expected an error for a payload with no reports field")
	}
	if !strings.Contains(err.Error(), "no report entries") {
		t.Fatalf("err = %v", err)
	}
}

func TestListReportsAcceptsAnEmptyReportList(t *testing.T) {
	// An authorized user who simply has no visible reports must not see an
	// error: the picker is legitimately empty.
	gw := newFakeGateway(t)
	gw.addTool(toolIDReports, descReports, func(map[string]interface{}) (string, bool) {
		return `{"total":0,"reports":[]}`, false
	})

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	defs, err := cli.listReports(context.Background(), toolIDReports, listQuery{DesignType: designTypeQuery})
	if err != nil {
		t.Fatalf("listReports: %v", err)
	}
	if len(defs) != 0 {
		t.Fatalf("defs = %+v", defs)
	}
}

func TestNormalizeRowHandlesEveryShapeItMaySee(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "query value array", raw: `["a","b"]`, want: []string{"a", "b"}},
		{name: "statistics values", raw: `{"values":["a","b"],"rowId":"1"}`, want: []string{"a", "b"}},
		{name: "null", raw: `null`, want: nil},
		{name: "empty string", raw: `""`, want: []string{""}},
		{name: "number", raw: `12`, want: []string{"12"}},
		{name: "boolean", raw: `true`, want: []string{"true"}},
		{name: "object without values becomes one json cell", raw: `{"x":1}`, want: []string{`{"x":1}`}},
		{name: "nested object cell is kept as json", raw: `["a",{"b":1}]`, want: []string{"a", `{"b":1}`}},
		{name: "mixed nulls", raw: `["a",null,"c"]`, want: []string{"a", "", "c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeRow(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatalf("normalizeRow: %v", err)
			}
			if !equalStrings(got, tc.want) && !(got == nil && tc.want == nil) {
				t.Fatalf("normalizeRow(%s) = %#v, want %#v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestFetchReportPagePadsRaggedRows(t *testing.T) {
	// The runtime can omit trailing empty cells; the rendered table must stay
	// rectangular so the Markdown header lines up with every row.
	report := &fakeReport{
		ID: "1", Name: "R", DesignType: designTypeQuery,
		Columns: []string{"a", "b", "c"},
		Rows:    [][]string{{"1"}, {"1", "2", "3", "4"}},
	}
	cli, _ := newFetchClient(t, report)
	page, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 1, 500, false, nil, nil)
	if err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	for i, row := range page.Rows {
		if len(row) != 4 {
			t.Fatalf("row %d has %d cells, want the widest row width 4: %v", i, len(row), page.Rows)
		}
	}
	if len(page.Columns) != 4 || page.Columns[3] != "列4" {
		t.Fatalf("missing column labels must be generated, got %v", page.Columns)
	}
}

func TestFetchReportPageNamesColumnsWhenTheRuntimeOmitsThem(t *testing.T) {
	report := &fakeReport{
		ID: "1", Name: "R", DesignType: designTypeQuery,
		Rows: [][]string{{"a", "b"}},
	}
	cli, _ := newFetchClient(t, report)
	page, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 1, 500, false, nil, nil)
	if err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if !equalStrings(page.Columns, []string{"列1", "列2"}) {
		t.Fatalf("columns = %v", page.Columns)
	}
}

func TestFetchReportPageComputesMissingTotalPages(t *testing.T) {
	// Some builds omit totalPages; the connector derives it so paging still
	// terminates correctly.
	gw := newFakeGateway(t)
	gw.addTool(toolIDQuery, descQuery, func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{
			"code": 200,
			"data": map[string]interface{}{
				"id": "1", "designType": designTypeQuery,
				"totalRecords": "10",
				"data": map[string]interface{}{
					"columns": []string{"a"},
					"rows":    []interface{}{[]interface{}{"x"}},
				},
			},
		}), false
	})
	gw.addTool(toolIDReports, descReports, func(map[string]interface{}) (string, bool) {
		return mustJSON(map[string]interface{}{"code": 200, "data": map[string]interface{}{"reports": []interface{}{}}}), false
	})
	gw.addTool(toolIDStats, descStats, nil)

	cli, err := newClient(gw.url(), testToken)
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if err := cli.resolveTools(context.Background(), nil); err != nil {
		t.Fatalf("resolveTools: %v", err)
	}
	page, err := cli.fetchReportPage(context.Background(), toolIDQuery, "1", designTypeQuery, 1, 4, false, nil, nil)
	if err != nil {
		t.Fatalf("fetchReportPage: %v", err)
	}
	if page.TotalPages != 3 {
		t.Fatalf("derived totalPages = %d, want 3", page.TotalPages)
	}
}

func TestFetchReportPageSurfacesAnUnknownReport(t *testing.T) {
	report := &fakeReport{ID: "1", Name: "R", DesignType: designTypeQuery, Columns: []string{"a"}}
	cli, _ := newFetchClient(t, report)
	_, err := cli.fetchReportPage(context.Background(), toolIDQuery, "missing", designTypeQuery, 1, 500, false, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "报表不存在") {
		t.Fatalf("want the gateway's business error, got %v", err)
	}
}

func TestRenderCellFlattensValues(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{raw: `"text"`, want: "text"},
		{raw: `""`, want: ""},
		{raw: `null`, want: ""},
		{raw: `42`, want: "42"},
		{raw: `42.5`, want: "42.5"},
		{raw: `false`, want: "false"},
		{raw: `{"a": 1}`, want: `{"a":1}`},
		{raw: `[1, 2]`, want: `[1,2]`},
		{raw: `"含|管道"`, want: "含|管道"},
	}
	for _, tc := range cases {
		if got := renderCell(json.RawMessage(tc.raw)); got != tc.want {
			t.Fatalf("renderCell(%s) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalStrings(a[i], b[i]) {
			return false
		}
	}
	return true
}
