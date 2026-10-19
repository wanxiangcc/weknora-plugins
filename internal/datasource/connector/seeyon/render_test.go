package seeyon

import (
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestMarkdownTableHeaderAndRows(t *testing.T) {
	head := markdownTableHeader([]string{"部门", "金额"})
	want := "| 部门 | 金额 |\n| --- | --- |\n"
	if head != want {
		t.Fatalf("header = %q, want %q", head, want)
	}
	if got := markdownRow([]string{"华东", "100"}); got != "| 华东 | 100 |" {
		t.Fatalf("row = %q", got)
	}
	// An empty row still renders a valid, non-degenerate table row.
	if got := markdownRow(nil); got != "| |" {
		t.Fatalf("empty row = %q", got)
	}
}

func TestEscapeCellKeepsTheTableIntact(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  string
	}{
		{name: "pipe", value: "a|b", want: `a\|b`},
		{name: "newline", value: "a\nb", want: "a<br>b"},
		{name: "crlf", value: "a\r\nb", want: "a<br>b"},
		{name: "cr", value: "a\rb", want: "a<br>b"},
		{name: "backslash", value: `a\b`, want: `a\\b`},
		{name: "plain", value: "正常文本", want: "正常文本"},
		{name: "empty", value: "", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapeCell(tc.value); got != tc.want {
				t.Fatalf("escapeCell(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}

	// A cell longer than the cap is truncated with a visible marker, and the cut
	// lands on a rune boundary so the document stays valid UTF-8.
	long := strings.Repeat("中", maxCellBytes)
	got := escapeCell(long)
	if !strings.Contains(got, "已截断") {
		t.Fatalf("an over-long cell must be marked as truncated")
	}
	if len(got) > maxCellBytes+64 {
		t.Fatalf("truncated cell is %d bytes", len(got))
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncation must not split a multi-byte rune")
	}
	// The cut is moved back to the nearest rune boundary, so up to two bytes of
	// content are given up rather than emitting invalid UTF-8.
	kept := strings.Count(got, "中") * 3
	if kept < maxCellBytes-2 || kept > maxCellBytes {
		t.Fatalf("kept %d bytes of a %d-byte cap", kept, maxCellBytes)
	}
}

func TestRendererSplitsByByteCeiling(t *testing.T) {
	// A 64 KiB ceiling is the smallest allowed document; with ~1 KiB rows it
	// must produce several parts, and every part must respect the ceiling.
	const ceiling = minMaxDocumentBytes
	renderer := newReportRenderer(ceiling, "1001", "销售报表", []string{"单据号", "备注"})

	var parts []*renderedPart
	bigCell := strings.Repeat("x", 900)
	for i := 0; i < 400; i++ {
		parts = append(parts, renderer.addRow([]string{fmt.Sprintf("NO-%04d", i), bigCell}, "")...)
	}
	if trailing := renderer.flush(); trailing != nil {
		parts = append(parts, trailing)
	}

	if len(parts) < 2 {
		t.Fatalf("a report larger than the ceiling must be split, got %d part(s)", len(parts))
	}
	if renderer.totalRowsAdded() != 400 {
		t.Fatalf("totalRowsAdded = %d, want 400", renderer.totalRowsAdded())
	}

	// Parts must tile the row range without gaps or overlaps.
	expectedNext := 1
	totalRows := 0
	for i, part := range parts {
		if part.Index != i+1 {
			t.Fatalf("part %d has index %d", i, part.Index)
		}
		if part.StartRow != expectedNext {
			t.Fatalf("part %d starts at row %d, want %d", i, part.StartRow, expectedNext)
		}
		if part.EndRow != part.StartRow+part.Rows-1 {
			t.Fatalf("part %d range %d–%d does not match its %d rows", i, part.StartRow, part.EndRow, part.Rows)
		}
		expectedNext = part.EndRow + 1
		totalRows += part.Rows
		if len(part.Content) > ceiling {
			t.Fatalf("part %d is %d bytes, above the %d ceiling", i, len(part.Content), ceiling)
		}
		if !strings.HasPrefix(string(part.Content), "# 销售报表（第 ") {
			t.Fatalf("part %d must carry its row range in the title: %q", i, firstLine(part.Content))
		}
		if !strings.Contains(string(part.Content), "| 单据号 | 备注 |") {
			t.Fatalf("part %d must repeat the table header", i)
		}
	}
	if totalRows != 400 {
		t.Fatalf("parts cover %d rows, want 400", totalRows)
	}
	if len(parts) > 1 && parts[0].EndRow == parts[len(parts)-1].EndRow {
		t.Fatal("parts must not all cover the same rows")
	}
}

func TestRendererSubtreeChildIDsAreStableAndPrefixed(t *testing.T) {
	renderer := newReportRenderer(minMaxDocumentBytes, "1001", "R", []string{"a"})
	var parts []*renderedPart
	for i := 0; i < 3; i++ {
		parts = append(parts, renderer.addRow([]string{"v"}, "")...)
	}
	if trailing := renderer.flush(); trailing != nil {
		parts = append(parts, trailing)
	}
	if len(parts) != 1 {
		t.Fatalf("a tiny report must be one part, got %d", len(parts))
	}
	if parts[0].ExternalID != "1001#part#0001" {
		t.Fatalf("external id = %q, want 1001#part#0001", parts[0].ExternalID)
	}
}

func TestRendererEmitsAnOversizedRowAsItsOwnPart(t *testing.T) {
	// A single row can exceed the ceiling even after per-cell truncation (a wide
	// report times the 8 KiB cell cap). It must still be emitted - dropping it
	// would be silent data loss - and must not loop forever, so it is flushed on
	// its own and flagged for the overview to explain.
	const ceiling = minMaxDocumentBytes
	columns := make([]string, 16)
	for i := range columns {
		columns[i] = fmt.Sprintf("c%d", i)
	}
	renderer := newReportRenderer(ceiling, "1001", "R", columns)

	wide := make([]string, len(columns))
	for i := range wide {
		wide[i] = strings.Repeat("y", maxCellBytes)
	}
	parts := renderer.addRow(wide, "")
	if trailing := renderer.flush(); trailing != nil {
		parts = append(parts, trailing)
	}

	if len(parts) != 1 {
		t.Fatalf("an oversized row must produce exactly one part, got %d", len(parts))
	}
	if !parts[0].Oversized {
		t.Fatal("the oversized part must be flagged")
	}
	if len(parts[0].Content) <= ceiling {
		t.Fatalf("this fixture is meant to exceed the ceiling, got %d bytes", len(parts[0].Content))
	}
	if parts[0].Rows != 1 || parts[0].StartRow != 1 || parts[0].EndRow != 1 {
		t.Fatalf("oversized part covers %d rows %d-%d", parts[0].Rows, parts[0].StartRow, parts[0].EndRow)
	}

	// The following, normal rows must continue in a fresh part rather than being
	// appended to the oversized one.
	normal := make([]string, len(columns))
	for i := range normal {
		normal[i] = "v"
	}
	more := renderer.addRow(normal, "")
	if trailing := renderer.flush(); trailing != nil {
		more = append(more, trailing)
	}
	if len(more) != 1 {
		t.Fatalf("the next row must start a new part, got %d", len(more))
	}
	if more[0].Index != 2 || more[0].StartRow != 2 {
		t.Fatalf("follow-up part = index %d rows %d-%d", more[0].Index, more[0].StartRow, more[0].EndRow)
	}
	if more[0].Oversized {
		t.Fatal("a normal row must not inherit the oversized flag")
	}
	if len(more[0].Content) > ceiling {
		t.Fatalf("the follow-up part is %d bytes", len(more[0].Content))
	}
}

func TestRendererProducesNothingForAnEmptyReport(t *testing.T) {
	renderer := newReportRenderer(minMaxDocumentBytes, "1001", "R", []string{"a"})
	if part := renderer.flush(); part != nil {
		t.Fatalf("an empty report must produce no parts, got %+v", part)
	}
}

func TestBuildOverviewDescribesTheReport(t *testing.T) {
	overview := buildOverview(overviewInput{
		Definition: reportDefinition{
			ID: "1001", Name: "销售报表", DesignType: designTypeStatistics,
			DesignTypeName: "统计报表", Category: "销售管理",
		},
		Columns:  []string{"部门", "金额"},
		FieldIDs: []string{"f1", "f2"},
		Parts: []partSummary{
			{ExternalID: "1001#part#0001", StartRow: 1, EndRow: 4113},
			{ExternalID: "1001#part#0002", StartRow: 4114, EndRow: 5000},
		},
		SourceTotal: 123456,
		EmittedRows: 5000,
		SyncedAt:    fixedSyncTime,
	})

	for _, want := range []string{
		"# 销售报表",
		"`1001`",
		"统计报表（STATISTICS）",
		"分类：销售管理",
		"来源总行数：123456",
		"本次同步行数：5000",
		"数据分片：2 个",
		"2026-10-16 09:40:51 UTC",
		"| 1 | 部门 | `f1` |",
		"| 1 | 第 1–4113 行 | `1001#part#0001` |",
		"| 2 | 第 4114–5000 行 | `1001#part#0002` |",
	} {
		if !strings.Contains(overview, want) {
			t.Fatalf("overview must contain %q:\n%s", want, overview)
		}
	}
	// A clean sync must not claim truncation.
	if strings.Contains(overview, "⚠️") {
		t.Fatalf("a full sync must carry no warning:\n%s", overview)
	}
}

func TestBuildOverviewMarksTruncationAndOversizedParts(t *testing.T) {
	overview := buildOverview(overviewInput{
		Definition:     reportDefinition{ID: "1", Name: "大报表", DesignType: designTypeQuery},
		Columns:        []string{"a"},
		Parts:          []partSummary{{ExternalID: "1#part#0001", StartRow: 1, EndRow: 10}},
		SourceTotal:    999999,
		EmittedRows:    10,
		Truncated:      true,
		PartLimitHit:   true,
		OversizedParts: 2,
		SyncedAt:       fixedSyncTime,
	})

	for _, want := range []string{
		"数据已截断",
		"本次仅同步前 10 行",
		"请调大该数据源的 max_rows 后重新同步",
		"分片数量已达上限",
		"有 2 个分片超过 max_document_bytes",
	} {
		if !strings.Contains(overview, want) {
			t.Fatalf("overview must contain %q:\n%s", want, overview)
		}
	}
	// The design type name falls back to a built-in label when the gateway did
	// not report one.
	if !strings.Contains(overview, "查询报表（QUERY）") {
		t.Fatalf("overview must label the design type:\n%s", overview)
	}
}

func TestBuildOverviewOmitsUnknownTotalsAndColumns(t *testing.T) {
	// A report whose size the gateway did not report must not claim "0 rows".
	overview := buildOverview(overviewInput{
		Definition:  reportDefinition{ID: "1", Name: "空报表"},
		SourceTotal: -1,
		SyncedAt:    fixedSyncTime,
	})
	if strings.Contains(overview, "来源总行数") {
		t.Fatalf("an unknown total must be omitted:\n%s", overview)
	}
	if strings.Contains(overview, "## 列定义") || strings.Contains(overview, "## 分片清单") {
		t.Fatalf("empty sections must be omitted:\n%s", overview)
	}
	if !strings.Contains(overview, "数据分片：0 个") {
		t.Fatalf("a report with no parts must say so:\n%s", overview)
	}
}

func TestNewReportRendererRejectsAnUnusableCeiling(t *testing.T) {
	// A sub-minimum ceiling is not reachable through parseConfig; the renderer
	// still falls back rather than producing one document per row.
	renderer := newReportRenderer(16, "1", "R", []string{"a"})
	if renderer.maxBytes != defaultMaxDocumentBytes {
		t.Fatalf("maxBytes = %d, want the default %d", renderer.maxBytes, defaultMaxDocumentBytes)
	}
}

func firstLine(content []byte) string {
	if index := strings.IndexByte(string(content), '\n'); index >= 0 {
		return string(content[:index])
	}
	return string(content)
}
