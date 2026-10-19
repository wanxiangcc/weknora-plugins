package seeyon

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
)

// A single report can hold hundreds of thousands of rows. Ingesting one
// knowledge document per row would mean hundreds of thousands of full
// parse → chunk → embed → index pipelines, so the snapshot is instead split
// into as many documents as the configured byte ceiling requires. Each document
// stays readable and retrievable on its own.

const (
	// headerReserve is slack reserved for the per-part title and the table
	// header, so a part composed at flush time still respects maxBytes.
	headerReserve = 512
	// maxCellBytes bounds one table cell. A rich-text or linked-table cell can
	// be arbitrarily large; without a cap a single cell could produce a
	// document far above the ceiling the operator configured. Truncation is
	// marked inside the cell so the loss is visible, never silent.
	maxCellBytes = 8192
	// maxPartsPerReport bounds the number of documents one report may produce.
	// At the minimum document size (64 KiB) that is ~4 GiB of rendered rows, so
	// it only trips on a pathological report; bounding it keeps the parent's
	// SubtreeKeep list and the deletion sweep finite.
	maxPartsPerReport = 65536
	// maxOverviewPartRows caps the part inventory in the overview document. In
	// per-row granularity every row is a part, so an uncapped list would make
	// the overview hundreds of thousands of lines — larger than the documents it
	// describes.
	maxOverviewPartRows = 200
)

// renderedPart is one closed document fragment.
type renderedPart struct {
	// Index is the 1-based part number within the report.
	Index int
	// StartRow and EndRow are the 1-based inclusive row range in the report.
	StartRow int
	EndRow   int
	// Rows is the number of data rows in this part.
	Rows int
	// ExternalID is the sub-item id, built with types.SubtreeChildID so the
	// parent's deletion sweep can reconcile it.
	ExternalID string
	// Oversized records that a single row already exceeded maxBytes, so this
	// part is larger than the configured ceiling. It is surfaced in the
	// overview so an operator can see why a document is large.
	Oversized bool
	Content   []byte
}

// reportRenderer accumulates rows into parts bounded by maxBytes.
type reportRenderer struct {
	maxBytes  int
	reportID  string
	title     string
	tableHead string

	// buf holds the data rows of the currently open part. The title and table
	// header are prepended in closePart, because a part's row range is only
	// known once the part ends.
	buf bytes.Buffer
	// partIdx is the number of parts closed so far; the open part is partIdx+1.
	partIdx int
	// partRows is the number of data rows in the currently open part.
	partRows int
	// totalRows is every row handed to the renderer so far.
	totalRows int
	// oversized marks the currently open part as exceeding maxBytes.
	oversized bool
	// fieldSelection summarizes which columns were kept, so the overview can
	// say "同步字段 12/58" instead of implying the report only had 12 columns.
	fieldSelection fieldSelection
	// rowMode emits one document per row instead of byte-bounded parts. The
	// bytes ceiling then only marks an oversized record, it does not split one.
	rowMode bool
	// columns maps a value to its label in row mode.
	columns []string
	// rowToken is the stable token of the buffered row (row mode).
	rowToken string
}

// fieldSelection records how the data source narrowed the report's columns.
type fieldSelection struct {
	// Selected is the number of columns actually synced.
	Selected int
	// Total is the number of columns the report exposes.
	Total int
	// Filtered reports whether fixed filter conditions were sent.
	Filtered bool
	// Fields is the configured selection (empty = all columns).
	Fields []string
}

func newReportRenderer(maxBytes int, reportID, title string, columns []string) *reportRenderer {
	if maxBytes < minMaxDocumentBytes {
		maxBytes = defaultMaxDocumentBytes
	}
	return &reportRenderer{
		maxBytes:  maxBytes,
		reportID:  reportID,
		title:     title,
		tableHead: markdownTableHeader(columns),
	}
}

// newRowRenderer builds a renderer that turns every row into its own document.
// Each row becomes a self-describing "字段：值" block, so a chunk cut from it
// still names its columns — which a Markdown table fragment does not.
func newRowRenderer(reportID, title string, columns []string) *reportRenderer {
	return &reportRenderer{
		reportID: reportID,
		title:    title,
		columns:  columns,
		rowMode:  true,
	}
}

// addRow appends one row and returns the parts that were closed as a result
// (usually none, one when the ceiling is crossed, and two when an oversized row
// both closes the previous part and stands alone). In row mode exactly one part
// is closed per row.
func (r *reportRenderer) addRow(row []string, token string) []*renderedPart {
	if r.rowMode {
		block := recordBlock(r.columns, row)
		r.buf.WriteString(block)
		r.partRows = 1
		r.totalRows++
		r.rowToken = sanitizeRowToken(token, r.totalRows)
		if r.maxBytes > 0 && r.buf.Len()+headerReserve > r.maxBytes {
			// A single record is never split across documents: mark it and keep
			// the whole record so no field silently disappears.
			r.oversized = true
		}
		if part := r.closePart(); part != nil {
			return []*renderedPart{part}
		}
		return nil
	}

	line := markdownRow(row)

	// Close the open part first if this row would push it past the ceiling.
	// The first row of a part is always accepted, so an oversized row can
	// never bounce forever between parts.
	var closed []*renderedPart
	if r.partRows > 0 {
		projected := r.buf.Len() + headerReserve + len(line) + 1
		if projected > r.maxBytes {
			if part := r.closePart(); part != nil {
				closed = append(closed, part)
			}
		}
	}

	r.buf.WriteString(line)
	r.buf.WriteByte('\n')
	r.partRows++
	r.totalRows++

	// A row that does not fit even in a part of its own is flushed immediately
	// so it cannot drag the following rows over the ceiling with it.
	if len(line)+1+headerReserve > r.maxBytes {
		r.oversized = true
		if part := r.closePart(); part != nil {
			closed = append(closed, part)
		}
	}
	return closed
}

// flush closes the trailing part, if any.
func (r *reportRenderer) flush() *renderedPart { return r.closePart() }

// totalRowsAdded reports how many rows were handed to the renderer.
func (r *reportRenderer) totalRowsAdded() int { return r.totalRows }

// partsClosed reports how many parts have been closed so far. The caller uses
// it to stop paging when maxPartsPerReport is reached.
func (r *reportRenderer) partsClosed() int { return r.partIdx }

// closePart composes the open part into a document, or returns nil when no row
// is buffered.
func (r *reportRenderer) closePart() *renderedPart {
	if r.partRows == 0 {
		return nil
	}
	r.partIdx++

	// The open part holds the last partRows of the rows seen so far.
	endRow := r.totalRows
	startRow := endRow - r.partRows + 1

	var out bytes.Buffer
	externalID := types.SubtreeChildID(r.reportID, "part", fmt.Sprintf("%04d", r.partIdx))
	if r.rowMode {
		// The row's own token makes the document id stable across inserts.
		fmt.Fprintf(&out, "# %s（记录 %s）\n\n", r.title, r.rowToken)
		externalID = types.SubtreeChildID(r.reportID, "row", r.rowToken)
	} else {
		fmt.Fprintf(&out, "# %s（第 %d–%d 行）\n\n", r.title, startRow, endRow)
		out.WriteString(r.tableHead)
	}
	out.Write(r.buf.Bytes())

	part := &renderedPart{
		Index:      r.partIdx,
		StartRow:   startRow,
		EndRow:     endRow,
		Rows:       r.partRows,
		ExternalID: externalID,
		Oversized:  r.oversized,
		Content:    out.Bytes(),
	}

	r.buf.Reset()
	r.partRows = 0
	r.oversized = false
	r.rowToken = ""
	return part
}

// recordBlock renders one row as self-describing lines. Empty cells are skipped
// instead of emitting "字段：" noise: a report with dozens of columns is mostly empty cells,
// and dropping them keeps each record compact enough to stay one chunk.
func recordBlock(columns []string, row []string) string {
	var out strings.Builder
	for i, cell := range row {
		value := strings.TrimSpace(cell)
		if value == "" {
			continue
		}
		name := ""
		if i < len(columns) {
			name = strings.TrimSpace(columns[i])
		}
		if name == "" {
			name = fmt.Sprintf("列%d", i+1)
		}
		out.WriteString(name)
		out.WriteString("：")
		out.WriteString(recordValue(value))
		out.WriteString("\n")
	}
	if out.Len() == 0 {
		// Every cell was blank; keep the record addressable rather than empty.
		out.WriteString("（空记录）\n")
	}
	return out.String()
}

// recordValue keeps a value on one line: a newline inside a value would break
// the "one field per line" reading the record block depends on.
func recordValue(value string) string {
	replacer := strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ")
	return replacer.Replace(value)
}

// sanitizeRowToken makes a row id usable as a subtree token: the sweep's '#' is
// the id separator, so it must not appear, and an empty token falls back to the
// row's position (stable only while the report's order is).
func sanitizeRowToken(token string, position int) string {
	token = strings.TrimSpace(token)
	token = strings.ReplaceAll(token, "#", "_")
	if token == "" {
		return fmt.Sprintf("R%06d", position)
	}
	if len(token) > 64 {
		token = token[:64]
	}
	return token
}

// markdownTableHeader renders the header and alignment rows.
func markdownTableHeader(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	var out strings.Builder
	out.WriteString("|")
	for _, column := range columns {
		out.WriteString(" ")
		out.WriteString(escapeCell(column))
		out.WriteString(" |")
	}
	out.WriteString("\n|")
	for range columns {
		out.WriteString(" --- |")
	}
	out.WriteString("\n")
	return out.String()
}

// markdownRow renders one data row.
func markdownRow(row []string) string {
	if len(row) == 0 {
		return "| |"
	}
	var out strings.Builder
	out.WriteString("|")
	for _, cell := range row {
		out.WriteString(" ")
		out.WriteString(escapeCell(cell))
		out.WriteString(" |")
	}
	return out.String()
}

// escapeCell makes a value safe inside a Markdown table cell: pipes would split
// the row and newlines would break the table, so both are replaced, and an
// over-long cell is truncated with a visible marker.
func escapeCell(value string) string {
	if value == "" {
		return ""
	}
	if len(value) > maxCellBytes {
		// Trim to a rune boundary so the rendered document stays valid UTF-8.
		cut := maxCellBytes
		for cut > 0 && !isUTF8Boundary(value, cut) {
			cut--
		}
		value = value[:cut] + "…（单元格内容过长已截断）"
	}
	return strings.NewReplacer(
		"\\", "\\\\",
		"|", "\\|",
		"\r\n", "<br>",
		"\n", "<br>",
		"\r", "<br>",
	).Replace(value)
}

// isUTF8Boundary reports whether value can be cut at index without splitting a
// multi-byte rune.
func isUTF8Boundary(value string, index int) bool {
	if index <= 0 || index >= len(value) {
		return true
	}
	return value[index]&0xC0 != 0x80
}

// partSummary is the per-part entry the overview lists.
type partSummary struct {
	ExternalID string
	StartRow   int
	EndRow     int
}

// overviewInput collects everything the parent document reports.
type overviewInput struct {
	Definition     reportDefinition
	Columns        []string
	FieldIDs       []string
	Fields         fieldSelection
	Parts          []partSummary
	SourceTotal    int
	EmittedRows    int
	Truncated      bool
	PartLimitHit   bool
	OversizedParts int
	SyncedAt       time.Time
}

// buildOverview renders the parent document of a report: one small document
// carrying the report's identity, columns, scale and part index. It is what
// makes a report discoverable ("这个报表有多少条、有哪些列") without reading any
// data document, and it is also the item that drives the stale-part sweep.
func buildOverview(in overviewInput) string {
	def := in.Definition
	var out strings.Builder
	fmt.Fprintf(&out, "# %s\n\n", def.Name)
	out.WriteString("- 报表 ID：`" + def.ID + "`\n")
	fmt.Fprintf(&out, "- 报表类型：%s（%s）\n", designTypeLabel(def), def.DesignType)
	if def.Category != "" {
		out.WriteString("- 分类：" + def.Category + "\n")
	}
	if in.SourceTotal >= 0 {
		fmt.Fprintf(&out, "- 来源总行数：%d\n", in.SourceTotal)
	}
	fmt.Fprintf(&out, "- 本次同步行数：%d\n", in.EmittedRows)
	fmt.Fprintf(&out, "- 数据分片：%d 个\n", len(in.Parts))
	if in.Fields.Total > 0 && in.Fields.Selected != in.Fields.Total {
		fmt.Fprintf(&out, "- 同步字段：%d / %d 列（其余列未纳入同步）\n",
			in.Fields.Selected, in.Fields.Total)
	} else if in.Fields.Total > 0 {
		fmt.Fprintf(&out, "- 同步字段：全部 %d 列\n", in.Fields.Total)
	}
	if in.Fields.Filtered {
		out.WriteString("- 固定筛选条件：已生效（条件只影响同步的数据，不改动报表本身）\n")
	}
	if in.Truncated {
		fmt.Fprintf(&out,
			"- ⚠️ 数据已截断：本次仅同步前 %d 行。如需更多数据，"+
				"请调大该数据源的 max_rows 后重新同步。\n", in.EmittedRows)
	}
	if in.PartLimitHit {
		fmt.Fprintf(&out,
			"- ⚠️ 分片数量已达上限 %d，剩余行未同步。请调大 max_document_bytes 或收窄报表范围。\n",
			maxPartsPerReport)
	}
	if in.OversizedParts > 0 {
		fmt.Fprintf(&out,
			"- ⚠️ 有 %d 个分片超过 max_document_bytes：其中存在单行数据本身就大于该上限。\n",
			in.OversizedParts)
	}
	fmt.Fprintf(&out, "- 同步时间：%s\n", in.SyncedAt.UTC().Format("2006-01-02 15:04:05 UTC"))
	out.WriteString("\n本数据源按行快照同步：整张报表被拆成多个分片文档，" +
		"每篇文档标题标注了它覆盖的行号区间。每次同步都是当时的快照，不保留历史版本。\n")

	if len(in.Columns) > 0 {
		out.WriteString("\n## 列定义\n\n| # | 列名 | 列 ID |\n| --- | --- | --- |\n")
		for i, column := range in.Columns {
			id := ""
			if i < len(in.FieldIDs) {
				id = in.FieldIDs[i]
			}
			fmt.Fprintf(&out, "| %d | %s | `%s` |\n", i+1, escapeCell(column), id)
		}
	}

	if len(in.Parts) > 0 {
		shown := len(in.Parts)
		if shown > maxOverviewPartRows {
			shown = maxOverviewPartRows
		}
		out.WriteString("\n## 分片清单\n\n| 分片 | 行区间 | 文档 ID |\n| --- | --- | --- |\n")
		for i, part := range in.Parts[:shown] {
			fmt.Fprintf(&out, "| %d | 第 %d–%d 行 | `%s` |\n",
				i+1, part.StartRow, part.EndRow, part.ExternalID)
		}
		if len(in.Parts) > shown {
			fmt.Fprintf(&out, "\n… 其余 %d 个分片未列出（共 %d 个）。\n",
				len(in.Parts)-shown, len(in.Parts))
		}
	}
	return out.String()
}

// designTypeLabel renders a human-readable label, preferring the name the
// gateway reported and falling back to a built-in mapping.
func designTypeLabel(def reportDefinition) string {
	if def.DesignTypeName != "" {
		return def.DesignTypeName
	}
	switch def.DesignType {
	case designTypeQuery, designTypeWeaQuery:
		return "查询报表"
	case designTypeStatistics, designTypeMultStats, designTypeScheduledStats, designTypeWeaStats:
		return "统计报表"
	case designTypeSummary:
		return "指标报表"
	default:
		return def.DesignType
	}
}
