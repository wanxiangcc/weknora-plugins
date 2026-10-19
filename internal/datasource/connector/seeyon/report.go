package seeyon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
)

// The report runtime wraps every payload in the OA envelope
// {"code":200,"message":"操作成功","data":…}; "data" is the report-specific
// response. Tolerating a bare payload keeps the connector working if a future
// tool returns one.
type oaEnvelope struct {
	// Code is a number on older gateway builds and a string on current ones
	// ({"code":"200"}), so it is decoded through the same lenient numeric type
	// the pagination fields use.
	Code    *flexibleInt    `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// unwrap strips the OA envelope and returns the inner payload.
func unwrap(tool string, payload json.RawMessage) (json.RawMessage, error) {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return nil, fmt.Errorf("seeyon tool %s returned an empty payload", tool)
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("seeyon tool %s returned malformed JSON: %s", tool, truncateForError(payload))
	}
	// Only an object can carry the OA envelope: a bare array is passed straight
	// through so the caller's own decoder reports what it actually needed,
	// instead of this layer blaming the envelope.
	if trimmed[0] != '{' {
		return payload, nil
	}
	// A payload with no "code" field is already the inner object.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(payload, &probe); err != nil {
		return nil, fmt.Errorf("seeyon tool %s returned an unusable payload: %w", tool, err)
	}
	if _, hasCode := probe["code"]; !hasCode {
		return payload, nil
	}

	var envelope oaEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("decode seeyon tool %s envelope: %w", tool, err)
	}
	if envelope.Code != nil && int(*envelope.Code) != 200 {
		return nil, fmt.Errorf(
			"seeyon tool %s returned code %d: %s",
			tool, int(*envelope.Code), strings.TrimSpace(envelope.Message),
		)
	}
	if len(envelope.Data) == 0 || strings.TrimSpace(string(envelope.Data)) == "null" {
		return nil, fmt.Errorf("seeyon tool %s returned no data: %s", tool, strings.TrimSpace(envelope.Message))
	}
	return envelope.Data, nil
}

// flexibleInt accepts both JSON numbers and the strings the report runtime uses
// for pagination and totals (a deliberate CAP4 compatibility quirk: page,
// pageSize, totalRecords and totalPages are all serialized as strings).
type flexibleInt int

func (f *flexibleInt) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" {
		*f = 0
		return nil
	}
	text = strings.Trim(text, `"`)
	if text == "" {
		*f = 0
		return nil
	}
	// A decimal string ("10.0") is not expected, but parse leniently rather
	// than failing a whole page over a formatting quirk.
	if value, err := strconv.ParseFloat(text, 64); err == nil {
		*f = flexibleInt(int(value))
		return nil
	}
	return fmt.Errorf("cannot parse %q as a number", text)
}

// rawReportDefinition mirrors the fields of a report definition this connector
// consumes. The field groups are only populated when the request asks for them:
// the picker listing switches them off, while the field step asks for display
// and filter fields specifically.
type rawReportDefinition struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	DesignType     string `json:"designType"`
	DesignTypeName string `json:"designTypeName"`
	Category       string `json:"category"`
	SupportsDrill  bool   `json:"supportsDrill"`
	SupportsSort   bool   `json:"supportsSort"`
	RecommendedAPI string `json:"recommendedApi"`

	DisplayFields []rawDisplayField `json:"displayFields"`
	FilterFields  []rawFilterField  `json:"filterFields"`
	RowHeaders    []rawDisplayField `json:"rowHeaders"`
	ColumnHeaders []rawDisplayField `json:"columnHeaders"`
}

// rawDisplayField is one column definition of a report.
type rawDisplayField struct {
	TableName    string      `json:"tableName"`
	FieldName    string      `json:"fieldName"`
	FieldDisplay string      `json:"fieldDisplay"`
	Index        flexibleInt `json:"index"`
}

// rawFilterField is one filterable field, with the operators the runtime accepts
// for it. Enumeration options let the filter editor offer a picker instead of
// asking the operator to paste an id.
type rawFilterField struct {
	TableName    string          `json:"tableName"`
	FieldName    string          `json:"fieldName"`
	FieldDisplay string          `json:"fieldDisplay"`
	FieldType    string          `json:"fieldType"`
	InputType    string          `json:"inputType"`
	IsEnum       bool            `json:"isEnum"`
	IsOrg        bool            `json:"isOrg"`
	Operators    []string        `json:"operators"`
	EnumOptions  []rawEnumOption `json:"enumOptions"`
}

type rawEnumOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type rawReportsResponse struct {
	Total   flexibleInt           `json:"total"`
	Reports []rawReportDefinition `json:"reports"`
}

// listQuery describes one report-listing request.
//
// DesignType is mandatory: the gateway answers "系统内部错误" when it is
// omitted, so the connector always asks one report kind at a time and merges.
type listQuery struct {
	DesignType string
	// NameLike narrows the listing server-side. The field step uses the report's
	// own name to avoid pulling all ~1600 definitions just to read one.
	NameLike string
	// IncludeDisplayFields / IncludeFilterFields pull the column and filter
	// definitions. The picker keeps both off: it only needs id and name, and
	// 2049 reports with fields is several megabytes.
	IncludeDisplayFields bool
	IncludeFilterFields  bool
}

// listReports fetches the reports the authorization code may read for one design
// type.
func (c *client) listReports(
	ctx context.Context, tool string, q listQuery,
) ([]reportDefinition, error) {
	args := map[string]interface{}{
		"designType":                 q.DesignType,
		"includeDisplayFields":       q.IncludeDisplayFields,
		"includeFilterFields":        q.IncludeFilterFields,
		"includeSortableFields":      false,
		"includeIndices":             false,
		"includeOutsideReportCenter": false,
	}
	if name := strings.TrimSpace(q.NameLike); name != "" {
		args["nameLike"] = name
	}
	payload, err := c.callTool(ctx, tool, args)
	if err != nil {
		return nil, err
	}
	inner, err := unwrap(tool, payload)
	if err != nil {
		return nil, err
	}

	// Accept both {"total":…,"reports":[…]} and a bare array.
	raw := rawReportsResponse{}
	if err := json.Unmarshal(inner, &raw); err != nil || raw.Reports == nil {
		var bare []rawReportDefinition
		if bareErr := json.Unmarshal(inner, &bare); bareErr != nil {
			if err != nil {
				return nil, fmt.Errorf("decode reports listing: %w", err)
			}
			return nil, fmt.Errorf("decode reports listing: no report entries in payload")
		}
		raw.Reports = bare
	}

	out := make([]reportDefinition, 0, len(raw.Reports))
	for _, entry := range raw.Reports {
		id := strings.TrimSpace(entry.ID)
		name := strings.TrimSpace(entry.Name)
		if id == "" {
			// A report without an id cannot be synced or selected; skipping it
			// must not fail the whole listing.
			logger.Warnf(ctx, "[Seeyon] skipping report %q: no id in the gateway listing", name)
			continue
		}
		designType := normalizeDesignType(entry.DesignType)
		if designType == "" {
			// An untyped entry is assumed to be a query report, matching the
			// apiRole default, so it stays visible instead of silently vanishing.
			designType = designTypeQuery
		}
		if !syncableDesignTypes[designType] {
			continue
		}
		if name == "" {
			name = id
		}
		out = append(out, reportDefinition{
			ID:             id,
			Name:           name,
			DesignType:     designType,
			DesignTypeName: strings.TrimSpace(entry.DesignTypeName),
			Category:       strings.TrimSpace(entry.Category),
			SupportsDrill:  entry.SupportsDrill,
			SupportsSort:   entry.SupportsSort,
			RecommendedAPI: strings.TrimSpace(entry.RecommendedAPI),
			DisplayFields:  entry.DisplayFields,
			FilterFields:   entry.FilterFields,
			RowHeaders:     entry.RowHeaders,
			ColumnHeaders:  entry.ColumnHeaders,
		})
	}
	return out, nil
}

// findReportDefinition looks one report up by id.
//
// The gateway's discovery tool can only filter by name (fuzzy) and design type,
// not by id, so the lookup is name-driven and then matched on the id. A report
// whose name matches several entries still resolves correctly, and a name the
// server normalises differently falls back to scanning the whole type.
func (c *client) findReportDefinition(
	ctx context.Context, tool, designType, reportID, reportName string,
) (*reportDefinition, error) {
	if strings.TrimSpace(reportName) != "" {
		found, err := c.listReports(ctx, tool, listQuery{
			DesignType:           designType,
			NameLike:             reportName,
			IncludeDisplayFields: true,
			IncludeFilterFields:  true,
		})
		if err != nil {
			return nil, err
		}
		for i := range found {
			if found[i].ID == reportID {
				return &found[i], nil
			}
		}
	}
	// Fallback: the name did not match (renamed, or normalised differently).
	all, err := c.listReports(ctx, tool, listQuery{
		DesignType:           designType,
		IncludeDisplayFields: true,
		IncludeFilterFields:  true,
	})
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].ID == reportID {
			return &all[i], nil
		}
	}
	return nil, fmt.Errorf(
		"%w: report %s is not in the current listing for design type %s",
		datasource.ErrInvalidConfig, reportID, designType,
	)
}

// rawStatisticsRow is the statistics-report row shape: an object whose values
// live under "values", alongside drill metadata this connector ignores.
type rawStatisticsRow struct {
	Values []json.RawMessage `json:"values"`
}

// ignoredFilter is a condition or sort the runtime refused to apply. The gateway
// does not fail the request: it drops the entry, echoes it here with a reason,
// and returns the *unfiltered* row set. That is the only feedback a filter was
// not honored, so it must never be ignored — the difference is syncing a slice
// versus syncing hundreds of thousands of rows.
type ignoredFilter struct {
	TableName string `json:"tableName"`
	FieldName string `json:"fieldName"`
	Op        string `json:"op"`
	Value     string `json:"value"`
	Order     string `json:"order"`
	Reason    string `json:"reason"`
}

type rawReportPage struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	DesignType        string          `json:"designType"`
	TotalRecords      flexibleInt     `json:"totalRecords"`
	TotalPages        flexibleInt     `json:"totalPages"`
	IgnoredConditions []ignoredFilter `json:"ignoredConditions"`
	IgnoredSorts      []ignoredFilter `json:"ignoredSorts"`
	Data              struct {
		Columns   []string          `json:"columns"`
		ColumnIDs []string          `json:"columnIds"`
		Rows      []json.RawMessage `json:"rows"`
		IDs       []string          `json:"ids"`
	} `json:"data"`
}

// fetchReportPage fetches one page and normalizes it.
//
// The page/pageSize arguments are serialized as strings on purpose: the CAP4
// runtime declares them as strings and the pageSize ceiling is enforced
// server-side. includeDesignType is set only for the legacy single-tool build,
// whose dispatcher needs it; the report-runtime tools route by tool name and
// would reject an unexpected designType.
func (c *client) fetchReportPage(
	ctx context.Context,
	tool, reportID, designType string,
	page, pageSize int,
	includeDesignType bool,
	conditions []reportCondition,
	sorts []reportSort,
) (*reportPage, error) {
	args := map[string]interface{}{
		"id":       reportID,
		"page":     strconv.Itoa(page),
		"pageSize": strconv.Itoa(pageSize),
	}
	if includeDesignType && designType != "" {
		args["designType"] = designType
	}
	// includeIds is only meaningful for query reports; requesting it keeps the
	// row ids available for a future open-detail link. The statistics tool
	// ignores unknown fields, and the query tool documents the field, so it is
	// always safe to ask.
	args["includeIds"] = true
	// A fixed filter applies to every page, so an incremental sync of a huge
	// report stays the same slice.
	if len(conditions) > 0 {
		args["conditions"] = conditions
	}
	// An explicit order makes the page boundaries of a snapshot reproducible.
	// The statistics tool has no sorts parameter, so the caller only passes one
	// for query reports.
	if len(sorts) > 0 {
		args["sorts"] = sorts
	}

	payload, err := c.callTool(ctx, tool, args)
	if err != nil {
		return nil, err
	}
	inner, err := unwrap(tool, payload)
	if err != nil {
		return nil, err
	}

	var decoded rawReportPage
	if err := json.Unmarshal(inner, &decoded); err != nil {
		return nil, fmt.Errorf("decode report page for %s: %w", reportID, err)
	}

	page0 := &reportPage{
		Columns:           decoded.Data.Columns,
		RowIDs:            decoded.Data.IDs,
		TotalRecords:      int(decoded.TotalRecords),
		TotalPages:        int(decoded.TotalPages),
		IgnoredConditions: decoded.IgnoredConditions,
		IgnoredSorts:      decoded.IgnoredSorts,
		Rows:              make([][]string, 0, len(decoded.Data.Rows)),
	}

	width := len(page0.Columns)
	for _, rawRow := range decoded.Data.Rows {
		cells, err := normalizeRow(rawRow)
		if err != nil {
			return nil, fmt.Errorf("decode report row for %s: %w", reportID, err)
		}
		if len(cells) > width {
			width = len(cells)
		}
		page0.Rows = append(page0.Rows, cells)
	}
	if width == 0 {
		// No columns and no rows: an empty report. Leave it empty so the
		// caller can still emit an overview document saying so.
		return page0, nil
	}
	page0.Columns = padColumns(page0.Columns, width)
	for i, row := range page0.Rows {
		page0.Rows[i] = padRow(row, width)
	}
	page0.FieldIDs = deriveFieldIDs(page0.Columns, decoded.Data.ColumnIDs, decoded.Data.Rows)
	if page0.TotalPages <= 0 && page0.TotalRecords > 0 && pageSize > 0 {
		page0.TotalPages = (page0.TotalRecords + pageSize - 1) / pageSize
	}
	return page0, nil
}

// deriveFieldIDs produces one stable id per column.
//
// Query reports return columnIds ("<tableName>_<fieldName>") directly. Statistics
// reports return null columnIds and carry the identity of each column in the
// drill metadata of every row instead, so the first row is used as the source.
// When neither is available (an empty statistics report, or a payload shape
// change) a positional "#<index>" keeps the columns addressable; that is enough
// to render and to match a saved selection, just not stable across a column
// reorder on the source side.
func deriveFieldIDs(columns, columnIDs []string, rawRows []json.RawMessage) []string {
	if len(columns) == 0 {
		return nil
	}
	if len(columnIDs) == len(columns) {
		return columnIDs
	}
	if len(rawRows) > 0 {
		if ids := fieldIDsFromDrill(rawRows[0]); len(ids) == len(columns) {
			return ids
		} else if len(ids) > 0 {
			// A short drill list still identifies the leading columns.
			out := make([]string, len(columns))
			for i := range out {
				if i < len(ids) && ids[i] != "" {
					out[i] = ids[i]
				} else {
					out[i] = positionalFieldID(i)
				}
			}
			return out
		}
	}
	out := make([]string, len(columns))
	for i := range out {
		out[i] = positionalFieldID(i)
	}
	return out
}

// fieldIDsFromDrill reads rows[0].drillContext.fieldValues[].{tableName,fieldName}.
// Statistics rows carry them for the row headers, column headers and statistic
// items alike, which is exactly the column order.
func fieldIDsFromDrill(raw json.RawMessage) []string {
	var row struct {
		DrillContext struct {
			FieldValues []struct {
				TableName string `json:"tableName"`
				FieldName string `json:"fieldName"`
			} `json:"fieldValues"`
		} `json:"drillContext"`
	}
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil
	}
	if len(row.DrillContext.FieldValues) == 0 {
		return nil
	}
	out := make([]string, 0, len(row.DrillContext.FieldValues))
	for _, fv := range row.DrillContext.FieldValues {
		out = append(out, fieldID(fv.TableName, fv.FieldName))
	}
	return out
}

func fieldID(tableName, fieldName string) string {
	tableName = strings.TrimSpace(tableName)
	fieldName = strings.TrimSpace(fieldName)
	if tableName == "" || fieldName == "" {
		return ""
	}
	return tableName + "_" + fieldName
}

func positionalFieldID(index int) string {
	return "#" + strconv.Itoa(index)
}

// normalizeRow accepts either of the two row encodings the report runtime uses:
// a query report's flat value array (["a","b"]) and a statistics report's
// object with a "values" array ({"values":["a","b"],…}). A scalar is treated as
// a single-cell row so a shape change degrades into visible data rather than a
// hard failure.
func normalizeRow(raw json.RawMessage) ([]string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	switch trimmed[0] {
	case '[':
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		return renderCells(values), nil
	case '{':
		var stats rawStatisticsRow
		if err := json.Unmarshal(raw, &stats); err != nil {
			return nil, err
		}
		if stats.Values == nil {
			// An object without "values" is not a row this connector knows how to
			// align to columns; render it as one JSON cell rather than dropping it.
			return []string{renderCell(raw)}, nil
		}
		return renderCells(stats.Values), nil
	default:
		return []string{renderCell(raw)}, nil
	}
}

func renderCells(values []json.RawMessage) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, renderCell(value))
	}
	return out
}

// renderCell flattens one JSON cell into a display string. Objects and arrays
// (enum tuples, linked-table cells) become compact JSON so no information is
// silently dropped from the snapshot.
func renderCell(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	switch trimmed[0] {
	case '"':
		var text string
		if err := json.Unmarshal(raw, &text); err == nil {
			return text
		}
		return trimmed
	case '{', '[':
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err == nil {
			return compact.String()
		}
		return trimmed
	default:
		return trimmed
	}
}

// padColumns fills in missing column names. The runtime always returns a column
// label, so this only matters when a report has rows but no declared columns.
func padColumns(columns []string, width int) []string {
	out := make([]string, width)
	for i := 0; i < width; i++ {
		if i < len(columns) {
			name := strings.TrimSpace(columns[i])
			if name == "" {
				name = fmt.Sprintf("列%d", i+1)
			}
			out[i] = name
			continue
		}
		out[i] = fmt.Sprintf("列%d", i+1)
	}
	return out
}

// padRow keeps every row the same width as the header, so the rendered table is
// well formed even when the runtime omits trailing empty cells.
func padRow(row []string, width int) []string {
	if len(row) == width {
		return row
	}
	out := make([]string, width)
	copy(out, row)
	return out
}

// changeMarker finds the CAP4 system field that records when a row last changed.
// It is filterable but never a display column, so it cannot be read out of the
// rows — it is only usable as a condition, which is exactly what the change
// probe needs. modify_date is preferred; start_date is the fallback for reports
// whose form does not maintain a modify timestamp.
func changeMarker(def reportDefinition) (tableName, fieldName string, ok bool) {
	for _, wanted := range []string{"modify_date", "start_date"} {
		for _, field := range def.FilterFields {
			if !strings.EqualFold(field.FieldName, wanted) {
				continue
			}
			if strings.TrimSpace(field.TableName) == "" {
				continue
			}
			return field.TableName, field.FieldName, true
		}
	}
	return "", "", false
}

// sortKey picks the column a snapshot is ordered by. Only query reports can be
// ordered, and only display columns are sortable, so the key must be a column
// that is both displayed and typed as a date. Sorting ascending by it keeps part
// boundaries stable: an inserted row lands at the end and only the trailing part
// is rewritten, whereas the runtime's default (newest first) shifts every row
// index and rewrites the whole report on every insert.
func sortKey(def reportDefinition) (tableName, fieldName string, ok bool) {
	dateFields := make(map[string]bool, len(def.FilterFields))
	for _, field := range def.FilterFields {
		switch strings.ToLower(strings.TrimSpace(field.InputType)) {
		case "datetime", "date":
			dateFields[field.TableName+"."+field.FieldName] = true
		}
	}
	for _, field := range def.DisplayFields {
		key := field.TableName + "." + field.FieldName
		if dateFields[key] {
			return field.TableName, field.FieldName, true
		}
	}
	return "", "", false
}
