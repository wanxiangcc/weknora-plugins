// Package seeyon implements the WeKnora data source connector for Seeyon V5
// (致远 OA) CAP4 reports.
//
// It talks to the Seeyon MCP gateway — a stateless streamable-HTTP JSON-RPC 2.0
// endpoint — and turns 查询类报表 (query reports) and 统计类报表 (statistics
// reports) into knowledge documents. Authentication is a per-user
// "MCP 个人授权码" carried in the Authorization header, the same credential the
// product prints into its copyable mcpServers JSON:
//
//	{"mcpServers":{"致远OA":{"type":"streamable_http",
//	  "url":"https://<gateway-host>/mcp-gateway/<application-id>/apps/seeyon/message",
//	  "headers":{"Authorization":"Bearer <授权码>"}}}}
//
// The gateway endpoint is deployment-specific and is therefore never baked into
// the connector: credentials.base_url is required and must be the URL printed
// by the operator's own V5 deployment.
//
// The gateway exposes tools under opaque identifiers (32-hex ids, or
// "sy-"/"cs-" + MD5), so this connector never hard-codes a tool name: it
// resolves the report tools from tools/list at run time and lets operators pin
// them through settings.tool_overrides.
package seeyon

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	// Embed the IANA timezone database: the probe watermark is rendered in the
	// OA server's zone (default Asia/Shanghai), and a slim container image has
	// no /usr/share/zoneinfo for time.LoadLocation to read.
	_ "time/tzdata"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	// defaultMaxDocumentBytes is the per-document byte ceiling. A report is
	// split into as many documents as needed to stay under it, so one
	// 300k-row report becomes tens of documents instead of 300k knowledge
	// items — each of which would otherwise pay the full
	// parse → chunk → embed → index pipeline.
	defaultMaxDocumentBytes = 1 << 20 // 1 MiB
	// The bounds keep an operator from configuring a document too small to
	// hold a header (thousands of documents) or too large for the ingest
	// pipeline to chunk in one pass.
	minMaxDocumentBytes = 64 << 10 // 64 KiB
	maxMaxDocumentBytes = 16 << 20 // 16 MiB

	// defaultPageSize / maxPageSize mirror the CAP4 runtime contract: pageSize
	// is hard-capped at 500 server-side (ComiReportRuntimeApiService), so a
	// larger value would be silently clamped and misreport progress.
	defaultPageSize = 500
	maxPageSize     = 500

	// defaultMaxRows is the safety valve for a single report in a single sync.
	// Reaching it stops pagination and marks the snapshot truncated; it is
	// never a silent discard.
	defaultMaxRows = 50000
	maxMaxRows     = 500000

	// maxEnumOptions caps the enumeration options returned with a filter field.
	maxEnumOptions = 200

	// maxPages bounds pagination independently of maxRows so a server that
	// reports a bogus/huge totalPages (or a forged totalRecords that never
	// decreases the remaining rows) cannot make one report loop forever.
	maxPages = 100000
)

// Tool roles. The connector resolves one concrete gateway tool name per role.
const (
	toolRoleReports    = "reports"
	toolRoleQuery      = "query"
	toolRoleStatistics = "statistics"
)

// toolRoles is the canonical, order-stable list used for validation and for
// building the "which tool is missing" error.
var toolRoles = []string{toolRoleReports, toolRoleQuery, toolRoleStatistics}

// CAP4 report design types. The values are the exact strings the report
// runtime accepts and returns.
const (
	designTypeQuery          = "QUERY"
	designTypeWeaQuery       = "WEAQUERY"
	designTypeStatistics     = "STATISTICS"
	designTypeMultStats      = "MULTSTATS"
	designTypeScheduledStats = "SCHEDULEDSTATS"
	designTypeWeaStats       = "WEASTATS"
	// designTypeSummary reports expose a single indicator value through the
	// summary-data tool rather than a row set, so they are recognised (to give
	// a precise "unsupported" message) but not syncable by this connector.
	designTypeSummary = "SUMMARY"
)

// syncableDesignTypes is what this connector can actually render: the two
// report kinds that come back as a plain row list.
//
//   - QUERY (查询报表) returns positional value arrays plus columnIds.
//   - STATISTICS (单表统计) returns {values, drillContext} objects whose field
//     identity has to be recovered from the drill metadata.
//
// The other CAP4 kinds are recognised so configuration can reject them with a
// precise reason instead of "unknown type", but they are out of scope: the
// multi-table variants pivot differently and the WEA* variants belong to a
// product this connector does not target.
var syncableDesignTypes = map[string]bool{
	designTypeQuery:      true,
	designTypeStatistics: true,
}

// defaultDesignTypes is the sync default: exactly the syncable kinds.
var defaultDesignTypes = []string{
	designTypeQuery,
	designTypeStatistics,
}

// knownDesignTypes is every type the runtime may return, used only to build a
// helpful rejection message.
var knownDesignTypes = func() map[string]bool {
	out := map[string]bool{}
	for _, t := range []string{
		designTypeQuery, designTypeWeaQuery, designTypeStatistics, designTypeMultStats,
		designTypeScheduledStats, designTypeWeaStats, designTypeSummary,
	} {
		out[t] = true
	}
	return out
}()

// statisticsDesignTypes selects the statistics tool; the query tool fetches
// everything else.
var statisticsDesignTypes = map[string]bool{
	designTypeStatistics: true,
}

// config is the connector-specific view of a data source configuration.
type config struct {
	// BaseURL is the MCP gateway streamable-HTTP endpoint.
	BaseURL string
	// AccessToken is the V5 MCP personal authorization code. It is a
	// user-scoped credential with a 30-day default validity and no refresh
	// flow, so an expired one surfaces as an actionable validation error.
	AccessToken string
	// DesignTypes narrows the sync to these report design types.
	DesignTypes []string
	// PageSize is the rows-per-request page size (never above maxPageSize).
	PageSize int
	// MaxDocumentBytes is the per-document byte ceiling used for splitting.
	MaxDocumentBytes int
	// MaxRows caps the rows fetched per report per sync. 0 means unlimited.
	MaxRows int
	// ToolOverrides pins a gateway tool name per role, bypassing discovery.
	ToolOverrides map[string]string
	// Fields narrows the synced columns to these field ids
	// ("<tableName>_<fieldName>"). Empty means every column the report
	// exposes, which is the default: a report with dozens of columns needs a way to keep
	// only the columns an operator cares about.
	Fields []string
	// Conditions are fixed filters applied to every page request. They let an
	// operator sync a slice of a report that is far larger than max_rows (the
	// target report has hundreds of thousands of rows) instead of only ever seeing the truncation
	// warning.
	Conditions []reportCondition
	// Granularity selects the document unit: granularityPart (default) packs
	// rows into byte-bounded parts, granularityRow emits one document per row.
	// Parts keep the document count in the hundreds; per-row documents give
	// exact citations at the cost of one ingest pipeline per row.
	Granularity string
	// ProbeIncremental enables the cheap "did anything change?" probe that
	// skips a whole sync round when the source is unchanged.
	ProbeIncremental bool
	// FullResyncHours bounds how long a probe may keep skipping: a full
	// snapshot is forced at least this often, so a source that does not
	// maintain its modify timestamp still converges.
	FullResyncHours int
	// TimestampTimezone is the timezone the gateway's report timestamps are in.
	// The probe watermark is rendered in it, so it does not depend on the sync
	// host's own local time. A watermark in the future would make every probe
	// see zero changes (bounded by FullResyncHours), which is why this is
	// explicit rather than inherited from the process.
	TimestampTimezone string
	// TimestampLocation is TimestampTimezone resolved once at configuration
	// time, so a bad zone fails config validation instead of every sync.
	TimestampLocation *time.Location
}

const (
	// granularityPart packs rows into byte-bounded part documents (default).
	granularityPart = "part"
	// granularityRow emits one document per row.
	granularityRow = "row"
)

const (
	defaultFullResyncHours = 24
	maxFullResyncHours     = 720 // 30 days
	// defaultTimestampTimezone is the OA deployment's timezone. It is the
	// default rather than the sync host's zone because the report timestamps
	// come from the OA server, not from this process.
	defaultTimestampTimezone = "Asia/Shanghai"
	// maxRowDocuments bounds per-row granularity. Every row becomes a child
	// document, so the parent's SubtreeKeep list holds one id per row; at
	// 50k ids the list is already ~1.5 MB in memory and the sweep does 50k
	// comparisons per sync.
	maxRowDocuments = 50000
)

// reportSort is one entry of the runtime's sorts array. Only the query tool
// accepts it; the statistics tool has no sorts parameter.
type reportSort struct {
	TableName string `json:"tableName"`
	FieldName string `json:"fieldName"`
	Order     string `json:"order"`
}

// reportCondition is one fixed filter passed straight through to the report
// runtime's conditions array.
type reportCondition struct {
	TableName string `json:"tableName"`
	FieldName string `json:"fieldName"`
	Op        string `json:"op"`
	Value     string `json:"value"`
}

// conditionOperators is the operator vocabulary both data tools declare. The
// spellings are the runtime's own (note "GreatEqual", not "GreaterEqual"), so
// they are matched case-insensitively but then emitted verbatim.
var conditionOperators = []string{
	"Equal", "NotEqual", "Like", "NotLike", "Include", "In", "IsNotNull",
	"Less", "Greater", "LessEqual", "GreatEqual",
}

// parseConfig builds and validates the connector configuration.
//
// The gateway endpoint and the access token are mandatory: this connector is
// deliberately shipped without a default URL, because the endpoint belongs to
// the operator's own deployment. Every other setting has a documented default.
func parseConfig(ds *types.DataSourceConfig) (*config, error) {
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}

	baseURL, _ := ds.Credentials["base_url"].(string)
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return nil, fmt.Errorf(
			"%w: credentials.base_url is required — copy the MCP gateway "+
				"endpoint printed by your V5 deployment's mcpServers configuration",
			datasource.ErrInvalidConfig,
		)
	}
	if err := datasource.ValidateConnectorBaseURL(baseURL); err != nil {
		// A self-hosted V5 gateway usually resolves to a private address, which
		// the SSRF policy blocks by default; name the allow-list here so the
		// operator does not have to reverse-engineer the rejection.
		return nil, fmt.Errorf(
			"%w: %v (for a gateway on a private network, add its host to the "+
				"deployment's SSRF_WHITELIST_EXTRA allow-list)",
			datasource.ErrInvalidConfig, err,
		)
	}

	accessToken, _ := ds.Credentials["access_token"].(string)
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return nil, fmt.Errorf(
			"%w: credentials.access_token is required — copy the MCP personal "+
				"authorization code from V5 个人设置 → 任务令牌",
			datasource.ErrInvalidConfig,
		)
	}

	out := &config{
		BaseURL:           baseURL,
		AccessToken:       accessToken,
		PageSize:          defaultPageSize,
		MaxDocumentBytes:  defaultMaxDocumentBytes,
		MaxRows:           defaultMaxRows,
		Granularity:       granularityPart,
		ProbeIncremental:  true,
		FullResyncHours:   defaultFullResyncHours,
		TimestampTimezone: defaultTimestampTimezone,
	}

	designTypes, err := parseDesignTypes(ds.Settings["design_types"])
	if err != nil {
		return nil, err
	}
	out.DesignTypes = designTypes

	if raw, ok := ds.Settings["page_size"]; ok && raw != nil {
		value, ok := settingInt(raw)
		if !ok || value < 1 {
			return nil, fmt.Errorf(
				"%w: settings.page_size must be a positive whole number",
				datasource.ErrInvalidConfig,
			)
		}
		if value > maxPageSize {
			// Clamp instead of rejecting: the server clamps anyway, and
			// rejecting would break a saved data source if the ceiling ever
			// moved down.
			value = maxPageSize
		}
		out.PageSize = value
	}

	if raw, ok := ds.Settings["max_document_bytes"]; ok && raw != nil {
		value, ok := settingInt(raw)
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.max_document_bytes must be a whole number of bytes",
				datasource.ErrInvalidConfig,
			)
		}
		if value < minMaxDocumentBytes || value > maxMaxDocumentBytes {
			return nil, fmt.Errorf(
				"%w: settings.max_document_bytes must be between %d and %d bytes",
				datasource.ErrInvalidConfig, minMaxDocumentBytes, maxMaxDocumentBytes,
			)
		}
		out.MaxDocumentBytes = value
	}

	if raw, ok := ds.Settings["max_rows"]; ok && raw != nil {
		value, ok := settingInt(raw)
		if !ok || value < 0 {
			return nil, fmt.Errorf(
				"%w: settings.max_rows must be zero or a positive whole number",
				datasource.ErrInvalidConfig,
			)
		}
		if value > maxMaxRows {
			value = maxMaxRows
		}
		out.MaxRows = value
	}

	overrides, err := parseToolOverrides(ds.Settings["tool_overrides"])
	if err != nil {
		return nil, err
	}
	out.ToolOverrides = overrides

	fields, err := parseFields(ds.Settings["fields"])
	if err != nil {
		return nil, err
	}
	out.Fields = fields

	conditions, err := parseConditions(ds.Settings["conditions"])
	if err != nil {
		return nil, err
	}
	out.Conditions = conditions

	if raw, ok := ds.Settings["document_granularity"]; ok && raw != nil {
		value, _ := raw.(string)
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", granularityPart:
			out.Granularity = granularityPart
		case granularityRow:
			out.Granularity = granularityRow
		default:
			return nil, fmt.Errorf(
				"%w: settings.document_granularity must be %q or %q",
				datasource.ErrInvalidConfig, granularityPart, granularityRow,
			)
		}
	}

	if raw, ok := ds.Settings["probe_incremental"]; ok && raw != nil {
		flag, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.probe_incremental must be true or false",
				datasource.ErrInvalidConfig,
			)
		}
		out.ProbeIncremental = flag
	}

	if raw, ok := ds.Settings["full_resync_hours"]; ok && raw != nil {
		value, ok := settingInt(raw)
		if !ok || value < 1 {
			return nil, fmt.Errorf(
				"%w: settings.full_resync_hours must be a positive whole number",
				datasource.ErrInvalidConfig,
			)
		}
		if value > maxFullResyncHours {
			value = maxFullResyncHours
		}
		out.FullResyncHours = value
	}

	if raw, ok := ds.Settings["timestamp_timezone"]; ok && raw != nil {
		value, isString := raw.(string)
		value = strings.TrimSpace(value)
		if !isString || value == "" {
			return nil, fmt.Errorf(
				"%w: settings.timestamp_timezone must be an IANA timezone name such as %q",
				datasource.ErrInvalidConfig, defaultTimestampTimezone,
			)
		}
		out.TimestampTimezone = value
	}
	// Resolve the zone once. time/tzdata is embedded in this package (see the
	// import block), so this works in a container without /usr/share/zoneinfo.
	location, err := time.LoadLocation(out.TimestampTimezone)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: settings.timestamp_timezone %q is not a known timezone; use an IANA name such as %q or %q",
			datasource.ErrInvalidConfig, out.TimestampTimezone, defaultTimestampTimezone, "UTC",
		)
	}
	out.TimestampLocation = location

	return out, nil
}

// parseFields reads settings.fields: the display fields to sync, identified by
// "<tableName>_<fieldName>". Absent or empty means "every column", which is the
// documented default. Blank entries are dropped and duplicates collapsed so a
// hand-edited config cannot distort the column order.
func parseFields(raw interface{}) ([]string, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: settings.fields must be an array", datasource.ErrInvalidConfig)
	}
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.fields must contain only strings",
				datasource.ErrInvalidConfig,
			)
		}
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

// parseConditions reads settings.conditions, the fixed filters sent with every
// page request.
func parseConditions(raw interface{}) ([]reportCondition, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf(
			"%w: settings.conditions must be an array of {tableName, fieldName, op, value}",
			datasource.ErrInvalidConfig,
		)
	}
	out := make([]reportCondition, 0, len(items))
	for i, item := range items {
		entry, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.conditions[%d] must be an object",
				datasource.ErrInvalidConfig, i,
			)
		}
		condition := reportCondition{
			TableName: settingString(entry["tableName"]),
			FieldName: settingString(entry["fieldName"]),
			Value:     settingString(entry["value"]),
		}
		op, err := normalizeOperator(entry["op"])
		if err != nil {
			return nil, fmt.Errorf("%w: settings.conditions[%d]: %v", datasource.ErrInvalidConfig, i, err)
		}
		condition.Op = op
		if condition.TableName == "" || condition.FieldName == "" {
			return nil, fmt.Errorf(
				"%w: settings.conditions[%d] needs both tableName and fieldName",
				datasource.ErrInvalidConfig, i,
			)
		}
		// IsNotNull takes no value; every other operator without one would
		// silently match nothing, which is harder to notice than a rejection.
		if condition.Op != "IsNotNull" && condition.Value == "" {
			return nil, fmt.Errorf(
				"%w: settings.conditions[%d] (%s) needs a value",
				datasource.ErrInvalidConfig, i, condition.Op,
			)
		}
		out = append(out, condition)
	}
	return out, nil
}

// normalizeOperator matches a configured operator against the runtime's own
// vocabulary, case-insensitively, and returns the canonical spelling.
func normalizeOperator(raw interface{}) (string, error) {
	value := strings.ToUpper(settingString(raw))
	for _, op := range conditionOperators {
		if strings.EqualFold(op, value) {
			return op, nil
		}
	}
	return "", fmt.Errorf(
		"unknown operator %q (accepted: %s)",
		settingString(raw), strings.Join(conditionOperators, ", "),
	)
}

// settingString reads a configuration value that may arrive as a string or, for
// a hand-written config, as a JSON number or bool.
func settingString(raw interface{}) string {
	switch typed := raw.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

// parseDesignTypes reads settings.design_types. An absent or empty value means
// "all syncable types"; a configured value is validated against the whitelist so
// a typo cannot silently narrow the sync to zero reports.
func parseDesignTypes(raw interface{}) ([]string, error) {
	if raw == nil {
		return append([]string(nil), defaultDesignTypes...), nil
	}
	items, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: settings.design_types must be an array", datasource.ErrInvalidConfig)
	}
	if len(items) == 0 {
		return append([]string(nil), defaultDesignTypes...), nil
	}
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		value, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.design_types must contain only strings",
				datasource.ErrInvalidConfig,
			)
		}
		normalized := normalizeDesignType(value)
		if normalized == "" {
			continue
		}
		if !knownDesignTypes[normalized] {
			return nil, fmt.Errorf(
				"%w: settings.design_types contains unknown type %q (accepted: %s)",
				datasource.ErrInvalidConfig, value, strings.Join(defaultDesignTypes, ", "),
			)
		}
		if !syncableDesignTypes[normalized] {
			// Known but out of scope: MULTSTATS and the WEA*/SCHEDULED variants
			// pivot differently or belong to another product, and SUMMARY returns
			// a single indicator rather than rows.
			return nil, fmt.Errorf(
				"%w: settings.design_types: %s reports are not syncable by this "+
					"connector (accepted: %s)",
				datasource.ErrInvalidConfig, normalized,
				strings.Join(defaultDesignTypes, ", "),
			)
		}
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		out = append(out, normalized)
	}
	if len(out) == 0 {
		return append([]string(nil), defaultDesignTypes...), nil
	}
	return out, nil
}

// parseToolOverrides reads settings.tool_overrides, rejecting unknown roles and
// blank values so a typo fails loudly at configuration time instead of turning
// into "tool not found" during a scheduled sync.
func parseToolOverrides(raw interface{}) (map[string]string, error) {
	if raw == nil {
		return nil, nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf(
			"%w: settings.tool_overrides must be an object",
			datasource.ErrInvalidConfig,
		)
	}
	out := make(map[string]string, len(m))
	for key, value := range m {
		role := strings.ToLower(strings.TrimSpace(key))
		if !isToolRole(role) {
			return nil, fmt.Errorf(
				"%w: settings.tool_overrides has unknown role %q (accepted: %s)",
				datasource.ErrInvalidConfig, key, strings.Join(toolRoles, ", "),
			)
		}
		name, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf(
				"%w: settings.tool_overrides.%s must be a string",
				datasource.ErrInvalidConfig, role,
			)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf(
				"%w: settings.tool_overrides.%s must not be blank",
				datasource.ErrInvalidConfig, role,
			)
		}
		out[role] = name
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func isToolRole(role string) bool {
	for _, r := range toolRoles {
		if r == role {
			return true
		}
	}
	return false
}

// normalizeDesignType upper-cases and trims a design type, treating null/blank
// as absent. The report runtime returns upper-case values, but configuration is
// hand-written, so accepting lower case avoids a pointless failure.
func normalizeDesignType(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

// apiRole maps a report design type to the tool role that fetches its data.
// An empty design type is treated as a query report: the reports listing always
// returns a designType, so this only covers a hand-written override or an
// older gateway that omits the field, and the query tool is the safer default
// (it accepts sorts and returns row ids).
func apiRole(designType string) string {
	if statisticsDesignTypes[normalizeDesignType(designType)] {
		return toolRoleStatistics
	}
	return toolRoleQuery
}

// reportDefinition is the subset of a report definition this connector uses.
type reportDefinition struct {
	ID             string
	Name           string
	DesignType     string
	DesignTypeName string
	Category       string
	SupportsDrill  bool
	SupportsSort   bool
	RecommendedAPI string
	// DisplayFields / FilterFields / RowHeaders / ColumnHeaders are populated
	// only by the field lookup (see client.findReportDefinition); the picker
	// listing leaves them empty to keep the resource list small.
	DisplayFields []rawDisplayField
	FilterFields  []rawFilterField
	// RowHeaders / ColumnHeaders describe a statistics report's dimensions.
	// Statistics reports have no displayFields, so these are the only
	// definition-level column information available.
	RowHeaders    []rawDisplayField
	ColumnHeaders []rawDisplayField
}

// fieldRef is one selectable column of a report, identified the same way
// settings.fields is: "<tableName>_<fieldName>".
type fieldRef struct {
	ID       string
	Label    string
	Role     string
	CalcType string
	Index    int
}

// Field roles reported by the statistics runtime's drill metadata.
const (
	fieldRoleRowHeader     = "rowHeader"
	fieldRoleColumnHeader  = "columnHeader"
	fieldRoleStatisticItem = "statisticItem"
)

// role reports which tool fetches this report's data. The definition's own
// recommendedApi wins when present because it is the server's own routing
// decision; the design type is the fallback.
func (d reportDefinition) role() string {
	switch strings.ToLower(strings.TrimSpace(d.RecommendedAPI)) {
	case toolRoleQuery:
		return toolRoleQuery
	case toolRoleStatistics:
		return toolRoleStatistics
	}
	return apiRole(d.DesignType)
}

// reportPage is one normalized page of report data: rows are string cells
// already aligned to Columns, whatever shape the upstream tool returned.
type reportPage struct {
	Columns []string
	// FieldIDs is the stable identity of each column, aligned to Columns:
	// "<tableName>_<fieldName>" when the runtime exposes it, else a positional
	// "#<index>" fallback. Query reports return real columnIds; statistics
	// reports do not, so the ids are recovered from the first row's drill
	// metadata. It is what settings.fields is matched against.
	FieldIDs []string
	Rows     [][]string
	RowIDs   []string
	// IgnoredConditions / IgnoredSorts are the conditions and sorts the runtime
	// refused to apply. A non-empty list means the data is wider than the
	// configuration asked for, so the sync treats it as a configuration error.
	IgnoredConditions []ignoredFilter
	IgnoredSorts      []ignoredFilter
	TotalRecords      int
	TotalPages        int
}

// reportCursor is the per-report progress recorded between syncs.
type reportCursor struct {
	// Parts maps a part's external_id to the SHA-256 of its content. An
	// unchanged digest lets the next incremental sync skip re-emitting the
	// part, which avoids a delete-and-recreate round of the full ingest
	// pipeline for data that did not move.
	Parts map[string]string `json:"parts,omitempty"`
	// Rows is the number of data rows emitted for this report last sync.
	Rows int `json:"rows"`
	// Total is the source's own row count last sync (before any truncation).
	Total int `json:"total"`
	// Truncated records that the last snapshot stopped at max_rows.
	Truncated bool `json:"truncated,omitempty"`
	// SyncedAt is when this report was last fetched.
	SyncedAt time.Time `json:"synced_at"`
	// Watermark is the change-probe lower bound, in the gateway's time
	// ("2006-01-02 15:04:05"). It is the *start* of the last full read, not its
	// end: a row modified mid-read may or may not have been captured, so the
	// next probe must still see it.
	Watermark string `json:"watermark,omitempty"`
	// ChangeField is the "table.field" change marker the watermark applies to
	// (the CAP4 modify_date system field). Empty disables probing.
	ChangeField string `json:"change_field,omitempty"`
	// SortField is the "table.field" the snapshot is ordered by. Ordering by an
	// ascending time field keeps part boundaries stable so an inserted row only
	// rewrites the trailing part.
	SortField string `json:"sort_field,omitempty"`
	// LastFullSync is when the last complete snapshot read started.
	LastFullSync time.Time `json:"last_full_sync,omitempty"`
}

// syncCursor is the connector cursor persisted in the data source row.
type syncCursor struct {
	Reports map[string]reportCursor `json:"reports,omitempty"`
}

// decodeCursor round-trips the service's untyped cursor map into syncCursor.
// A malformed cursor is reported as an empty state rather than an error: the
// worst case is one full re-sync, which is always safe, whereas failing here
// would wedge the data source until an operator cleared the row.
func decodeCursor(cursor *types.SyncCursor) (*syncCursor, []byte, error) {
	if cursor == nil || len(cursor.ConnectorCursor) == 0 {
		return &syncCursor{}, nil, nil
	}
	raw, err := json.Marshal(cursor.ConnectorCursor)
	if err != nil {
		return &syncCursor{}, nil, err
	}
	var decoded syncCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return &syncCursor{}, raw, err
	}
	return &decoded, raw, nil
}

// encodeCursor renders the connector cursor back into the service's shape.
func (c *syncCursor) encode(lastSync time.Time) *types.SyncCursor {
	payload := make(map[string]interface{})
	raw, err := json.Marshal(c)
	if err == nil {
		_ = json.Unmarshal(raw, &payload)
	}
	return &types.SyncCursor{
		LastSyncTime:    lastSync,
		ConnectorCursor: payload,
	}
}

// settingInt coerces an untyped settings value into an int. JSON numbers decode
// to float64, but a hand-written config, a cursor round trip or a form post can
// produce other shapes, so strings and the remaining numeric types are accepted
// and anything else is rejected rather than silently defaulted.
func settingInt(raw interface{}) (int, bool) {
	switch typed := raw.(type) {
	case float64:
		if typed != math.Trunc(typed) || typed > math.MaxInt32 || typed < math.MinInt32 {
			return 0, false
		}
		return int(typed), true
	case float32:
		return settingInt(float64(typed))
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		if typed > math.MaxInt32 || typed < math.MinInt32 {
			return 0, false
		}
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil || parsed > math.MaxInt32 || parsed < math.MinInt32 {
			return 0, false
		}
		return int(parsed), true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		var parsed json.Number = json.Number(trimmed)
		return settingInt(parsed)
	default:
		return 0, false
	}
}

// projectColumns narrows the report's columns to the configured field selection
// and reports which column indexes survive.
//
// It returns the kept column labels, their field ids, and the index list. A nil
// index list means "keep every column", which is both the empty selection and
// the fallback when nothing matched: an operator who narrowed the columns and
// then saw the report change under them is better served by a full sync plus a
// warning than by an empty document.
func projectColumns(
	ctx context.Context, columns, fieldIDs, wanted []string,
) ([]string, []string, []int) {
	if len(wanted) == 0 {
		return columns, fieldIDs, nil
	}
	byID := make(map[string]int, len(fieldIDs))
	for i, id := range fieldIDs {
		if _, exists := byID[id]; !exists {
			byID[id] = i
		}
	}
	indexes := make([]int, 0, len(wanted))
	missing := make([]string, 0)
	seen := make(map[int]bool, len(wanted))
	for _, id := range wanted {
		index, ok := byID[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		if seen[index] {
			continue
		}
		seen[index] = true
		indexes = append(indexes, index)
	}
	if len(missing) > 0 {
		logger.Warnf(ctx,
			"[Seeyon] %d configured field(s) are not in the report's current columns "+
				"and were ignored: %s", len(missing), strings.Join(missing, ", "))
	}
	if len(indexes) == 0 {
		logger.Warnf(ctx,
			"[Seeyon] none of the %d configured fields exist in the report any more; "+
				"syncing all %d columns instead", len(wanted), len(columns))
		return columns, fieldIDs, nil
	}
	sort.Ints(indexes)
	keptColumns := make([]string, 0, len(indexes))
	keptIDs := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < len(columns) {
			keptColumns = append(keptColumns, columns[index])
		} else {
			keptColumns = append(keptColumns, "")
		}
		if index < len(fieldIDs) {
			keptIDs = append(keptIDs, fieldIDs[index])
		} else {
			keptIDs = append(keptIDs, positionalFieldID(index))
		}
	}
	return keptColumns, keptIDs, indexes
}

// projectRow keeps the cells of the selected columns, in the same order.
func projectRow(row []string, indexes []int) []string {
	if len(indexes) == 0 {
		return row
	}
	out := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index < len(row) {
			out = append(out, row[index])
		} else {
			out = append(out, "")
		}
	}
	return out
}
