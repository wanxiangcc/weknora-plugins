package seeyon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// Compile-time proof that the connector satisfies every interface the framework
// looks for. StreamingConnector is what the sync service prefers, so a large
// report is ingested part-by-part with a resumable checkpoint instead of being
// materialized in memory; FullStreamingConnector keeps the stored cursor as the
// deletion baseline on a force-full run.
var (
	_ datasource.Connector              = (*Connector)(nil)
	_ datasource.StreamingConnector     = (*Connector)(nil)
	_ datasource.FullStreamingConnector = (*Connector)(nil)
)

// Connector syncs Seeyon V5 CAP4 report data (查询类报表 and 统计类报表) through
// the Seeyon MCP gateway. It is stateless: every data source carries its own
// gateway URL and MCP personal authorization code in its encrypted credentials.
type Connector struct {
	client *client
	cfg    *config
	// reports indexes the report definitions this data source may sync, keyed
	// by report id. It is populated from the gateway listing so a selected id
	// can be resolved to its design type and recommended API.
	reports map[string]reportDefinition
}

// NewConnector creates a stateless connector.
func NewConnector() *Connector { return &Connector{} }

// Type returns the connector type identifier.
func (c *Connector) Type() string { return types.ConnectorTypeSeeyon }

// newConfiguredClient validates the configuration and prepares the gateway
// client: initialize (which doubles as the credential and reachability probe)
// and tools/list (to resolve the opaque report tool ids).
//
// It deliberately stops before the report index. The index is one full listing
// per design type, and that listing costs ~24s on a tenant with well over a thousand reports
// because the gateway checks permissions report by report. Every caller that
// knows which report it wants (the picker's field step, the search, a sync)
// therefore resolves that one report by name instead of listing everything.
func newConfiguredClient(
	ctx context.Context, ds *types.DataSourceConfig,
) (*client, *config, error) {
	cfg, err := parseConfig(ds)
	if err != nil {
		return nil, nil, err
	}
	cli, err := newClient(cfg.BaseURL, cfg.AccessToken)
	if err != nil {
		return nil, nil, err
	}
	if err := cli.initialize(ctx); err != nil {
		return nil, nil, err
	}
	if err := cli.resolveTools(ctx, cfg.ToolOverrides); err != nil {
		return nil, nil, err
	}
	return cli, cfg, nil
}

// configured produces a per-run connector with the full report index.
//
// Only the paths that genuinely need every report use it (the unfiltered
// listing and the name-hint-less lookups). Keeping it separate from
// newConfiguredClient is what makes the picker fast: the field step used to pay
// this index for one report.
func (c *Connector) configured(ctx context.Context, ds *types.DataSourceConfig) (*Connector, error) {
	cli, cfg, err := newConfiguredClient(ctx, ds)
	if err != nil {
		return nil, err
	}
	index, err := listReportIndex(ctx, cli, cfg.DesignTypes)
	if err != nil {
		return nil, err
	}
	return &Connector{client: cli, cfg: cfg, reports: index}, nil
}

// resolveReport finds one report's definition by id.
//
// With a name hint it asks the gateway for that name only: nameLike is applied
// before the gateway builds a definition and checks its permission, so this is
// one cheap call per configured design type instead of the ~24s full listing.
// Without a hint it falls back to listing every configured type — correct, but
// that is exactly the cost the picker avoids by sending the hint.
func resolveReport(
	ctx context.Context, cli *client, cfg *config, reportID, nameHint string,
) (reportDefinition, error) {
	tool := cli.tools[toolRoleReports]
	if strings.TrimSpace(tool) == "" {
		return reportDefinition{}, fmt.Errorf(
			"%w: no resolved report listing tool", errToolNotFound)
	}
	if hint := strings.TrimSpace(nameHint); hint != "" {
		var lastErr error
		for _, designType := range cfg.DesignTypes {
			defs, listErr := cli.listReports(ctx, tool, listQuery{
				DesignType:           designType,
				NameLike:             hint,
				IncludeDisplayFields: true,
				IncludeFilterFields:  true,
			})
			if listErr != nil {
				lastErr = listErr
				continue
			}
			for i := range defs {
				if defs[i].ID == reportID {
					return defs[i], nil
				}
			}
		}
		if lastErr != nil {
			return reportDefinition{}, lastErr
		}
		return reportDefinition{}, fmt.Errorf(
			"%w: report %s was not found by name %q; the report may have been "+
				"renamed — re-select it in the data source",
			datasource.ErrInvalidConfig, reportID, hint)
	}
	var lastErr error
	for _, designType := range cfg.DesignTypes {
		defs, listErr := cli.listReports(ctx, tool, listQuery{DesignType: designType})
		if listErr != nil {
			lastErr = listErr
			continue
		}
		for i := range defs {
			if defs[i].ID == reportID {
				return defs[i], nil
			}
		}
	}
	if lastErr != nil {
		return reportDefinition{}, lastErr
	}
	return reportDefinition{}, fmt.Errorf(
		"%w: report %s is not in the current listing", datasource.ErrInvalidConfig, reportID)
}

// reportNameHint reads the display-only report name the picker saves next to the
// selection. It is a hint, never a key: the sync still addresses the report by
// resource_ids, and a stale hint only costs a slower lookup.
func reportNameHint(ds *types.DataSourceConfig) string {
	if ds == nil {
		return ""
	}
	raw, ok := ds.Settings["report_name"]
	if !ok {
		return ""
	}
	name, _ := raw.(string)
	return strings.TrimSpace(name)
}

// listReportIndex lists every configured design type into one id index.
//
// One listing per design type: the gateway needs designType explicitly (it
// answers 系统内部错误 when it is missing), so the kinds are queried in turn.
func listReportIndex(
	ctx context.Context, cli *client, designTypes []string,
) (map[string]reportDefinition, error) {
	var index map[string]reportDefinition
	err := forEachListingTool(ctx, cli, func(tool string) error {
		attempt := make(map[string]reportDefinition)
		for _, designType := range designTypes {
			defs, listErr := cli.listReports(ctx, tool, listQuery{DesignType: designType})
			if listErr != nil {
				return listErr
			}
			for _, def := range defs {
				attempt[def.ID] = def
			}
		}
		index = attempt
		return nil
	})
	if err != nil {
		return nil, err
	}
	return index, nil
}

// forEachListingTool runs fn against each candidate report listing tool until
// one succeeds, then records the winner on the client.
//
// Role resolution reads tool descriptions, so a gateway that rewords them can
// hand the listing role a data tool; that shows up as the opaque
// 报表ID和报表名称至少传入一个 on what the operator thinks is a connectivity
// test. The listing call is cheap and a wrong tool fails immediately, which
// makes trying the next candidate cheaper than failing.
func forEachListingTool(
	ctx context.Context, cli *client, fn func(tool string) error,
) error {
	candidates := cli.rankedTools(toolRoleReports, 3)
	if len(candidates) == 0 {
		return fmt.Errorf(
			"%w: no tool among tools/list looked like the report listing; "+
				"pin it via settings.tool_overrides.reports", errToolNotFound)
	}
	var lastErr error
	for i, tool := range candidates {
		lastErr = fn(tool)
		if lastErr == nil {
			if i > 0 {
				logger.Infof(ctx, "[Seeyon] using fallback report listing tool %s", tool)
			}
			cli.tools[toolRoleReports] = tool
			return nil
		}
		if i+1 < len(candidates) {
			logger.Warnf(ctx,
				"[Seeyon] report listing via tool %s failed (%v); trying %s",
				tool, lastErr, candidates[i+1])
		}
	}
	return hintRejectedListing(lastErr, candidates)
}

// SearchResources answers the report picker with matches only.
//
// The picker used to load the whole listing and filter in the browser, which is
// a listing of well over a thousand reports per visit and, with full definitions, has already
// timed out the gateway at 504. An empty keyword therefore returns nothing at
// all: "search everything" is exactly what this method exists to avoid.
func (c *Connector) SearchResources(
	ctx context.Context, ds *types.DataSourceConfig, keyword string,
) ([]types.Resource, error) {
	trimmed := strings.TrimSpace(keyword)
	if trimmed == "" {
		return []types.Resource{}, nil
	}
	cli, cfg, err := newConfiguredClient(ctx, ds)
	if err != nil {
		return nil, err
	}

	var results []types.Resource
	err = forEachListingTool(ctx, cli, func(tool string) error {
		attempt := make([]types.Resource, 0, 16)
		seen := make(map[string]bool)
		for _, designType := range cfg.DesignTypes {
			defs, listErr := cli.listReports(ctx, tool, listQuery{
				DesignType: designType,
				NameLike:   trimmed,
			})
			if listErr != nil {
				return listErr
			}
			for _, def := range defs {
				if def.ID == "" || seen[def.ID] {
					continue
				}
				seen[def.ID] = true
				attempt = append(attempt, reportResource(def))
			}
		}
		results = attempt
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].Name != results[j].Name {
			return results[i].Name < results[j].Name
		}
		return results[i].ExternalID < results[j].ExternalID
	})
	return results, nil
}

// hintRejectedListing turns "the gateway answered an error" into something an
// operator can act on. The common cause is the wrong tool: a data tool requires
// a report id, which a listing call has no way to send.
func hintRejectedListing(lastErr error, tried []string) error {
	if lastErr == nil {
		return fmt.Errorf("%w: report listing failed for an unknown reason", errToolNotFound)
	}
	hint := "none of the candidate listing tools worked"
	if strings.Contains(lastErr.Error(), "报表ID") || strings.Contains(lastErr.Error(), "报表名称") {
		hint = "the resolved tool requires a report id, so it is a report *data* tool rather " +
			"than the listing tool; pin the listing tool via settings.tool_overrides.reports"
	}
	// Both causes matter to the caller: the gateway error says what happened,
	// errToolNotFound says the tool choice is what needs fixing.
	return fmt.Errorf("%w: %w (%s; tried %s)",
		errToolNotFound, lastErr, hint, strings.Join(tried, ", "))
}

// Validate proves the endpoint is reachable, the authorization code is accepted,
// the report tools are available to it and the listing call actually works. The
// last part matters: role resolution is description-based, so only a real
// listing proves the chosen tool is the right one.
//
// Validate runs on the credentials step, before any report is selected, so it
// must never call a report data tool — there is no report id to send.
func (c *Connector) Validate(ctx context.Context, ds *types.DataSourceConfig) error {
	cli, cfg, err := newConfiguredClient(ctx, ds)
	if err != nil {
		return err
	}
	// One sentinel-filtered listing proves the same three things the full index
	// proves — reachable, authorized, and the resolved tool really lists reports
	// — but the gateway applies nameLike before it builds a definition and checks
	// its permission, so this takes ~1s instead of ~24s. A data tool would still
	// reject it (it needs a report id), which is exactly the mis-resolution this
	// has to catch.
	return forEachListingTool(ctx, cli, func(tool string) error {
		_, listErr := cli.listReports(ctx, tool, listQuery{
			DesignType: cfg.DesignTypes[0],
			NameLike:   validateSentinelName,
		})
		return listErr
	})
}

// validateSentinelName is a name no report can have: it keeps the gateway's
// per-report permission and definition work to zero while still exercising the
// listing call. Chinese text is deliberate — it proves the round trip handles
// non-ASCII arguments.
const validateSentinelName = "连接测试-不存在的报表-请忽略"

// ListResources lists reports at the top level, and a report's fields when asked
// for that report as the parent.
//
// The second level exists for the field-selection step: it returns both the
// selectable columns ("display_field") and the filterable definitions
// ("filter_field"), so one call feeds the column picker and the fixed-filter
// editor. Reports themselves advertise HasChildren=false — nothing expands them
// inside the resource tree, because the field step asks for them explicitly.
func (c *Connector) ListResources(
	ctx context.Context, ds *types.DataSourceConfig, parentID string,
) ([]types.Resource, error) {
	if reportID := strings.TrimSpace(parentID); reportID != "" {
		return c.reportFields(ctx, ds, reportID, reportNameHint(ds))
	}
	configured, err := c.configured(ctx, ds)
	if err != nil {
		return nil, err
	}
	return configured.resources(), nil
}

// ResolveResourceFields answers the field step with the name hint the picker has
// at hand, so the lookup costs one filtered listing instead of the full index.
func (c *Connector) ResolveResourceFields(
	ctx context.Context, ds *types.DataSourceConfig, resourceID, nameHint string,
) ([]types.Resource, error) {
	reportID := strings.TrimSpace(resourceID)
	if reportID == "" {
		return nil, fmt.Errorf("%w: a report id is required", datasource.ErrInvalidConfig)
	}
	if hint := strings.TrimSpace(nameHint); hint != "" {
		return c.reportFields(ctx, ds, reportID, hint)
	}
	return c.reportFields(ctx, ds, reportID, reportNameHint(ds))
}

// reportFields returns one report's selectable columns and filterable fields.
//
// Columns come from the report definition when it has displayFields, which is
// every QUERY report: the definition is already in hand and its displayFields
// are the same ordered list the data rows carry (verified: 58/58 identical to
// data.columns, and tableName_fieldName equals columnIds). Only statistics
// reports — which have no displayFields at all, their columns exist solely in a
// row's drill metadata — pay for a single-row data probe. That probe was ~37s on
// a report with hundreds of thousands of rows, so skipping it for QUERY reports is the difference between
// a 62s field step and a ~1s one.
func (c *Connector) reportFields(
	ctx context.Context, ds *types.DataSourceConfig, reportID, nameHint string,
) ([]types.Resource, error) {
	cli, cfg, err := newConfiguredClient(ctx, ds)
	if err != nil {
		return nil, err
	}
	def, err := resolveReport(ctx, cli, cfg, reportID, nameHint)
	if err != nil {
		return nil, err
	}
	full, err := cli.findReportDefinition(
		ctx, cli.tools[toolRoleReports], def.DesignType, def.ID, def.Name,
	)
	if err != nil {
		return nil, err
	}

	columns, columnIDs := columnsFromDefinition(full)
	if len(columns) == 0 {
		role := def.role()
		tool := cli.tools[role]
		if strings.TrimSpace(tool) == "" {
			return nil, fmt.Errorf("%w: no resolved tool for role %s", errToolNotFound, role)
		}
		includeDesignType := cli.legacyTool != "" && tool == cli.legacyTool
		probe, probeErr := cli.fetchReportPage(
			ctx, tool, def.ID, def.DesignType, 1, 1, includeDesignType, nil, nil,
		)
		if probeErr != nil {
			return nil, probeErr
		}
		columns, columnIDs = probe.Columns, probe.FieldIDs
	}

	out := make([]types.Resource, 0, len(columns)+len(full.FilterFields))
	for i, label := range columns {
		id := positionalFieldID(i)
		if i < len(columnIDs) && columnIDs[i] != "" {
			id = columnIDs[i]
		}
		out = append(out, types.Resource{
			ExternalID: id,
			Name:       label,
			Type:       "display_field",
			ParentID:   def.ID,
			Metadata: map[string]interface{}{
				"index": i,
			},
		})
	}
	for _, filter := range full.FilterFields {
		id := fieldID(filter.TableName, filter.FieldName)
		if id == "" {
			continue
		}
		label := strings.TrimSpace(filter.FieldDisplay)
		if label == "" {
			label = id
		}
		metadata := map[string]interface{}{
			"table_name": filter.TableName,
			"field_name": filter.FieldName,
			"input_type": filter.InputType,
			"is_enum":    filter.IsEnum,
			"operators":  filter.Operators,
		}
		// Cap the option list: a populated org or enum field can carry
		// thousands of entries, and the picker only needs a usable subset.
		if len(filter.EnumOptions) > 0 {
			options := filter.EnumOptions
			if len(options) > maxEnumOptions {
				options = options[:maxEnumOptions]
			}
			rendered := make([]map[string]string, 0, len(options))
			for _, option := range options {
				rendered = append(rendered, map[string]string{
					"id":    option.ID,
					"label": option.Label,
				})
			}
			metadata["enum_options"] = rendered
		}
		out = append(out, types.Resource{
			ExternalID: id,
			Name:       label,
			Type:       "filter_field",
			ParentID:   def.ID,
			Metadata:   metadata,
		})
	}
	return out, nil
}

// ResolveResourceAncestors has nothing to reveal: reports are a flat list with
// no nesting, so a selection has no ancestors to expand.
func (c *Connector) ResolveResourceAncestors(
	context.Context, *types.DataSourceConfig, []string,
) ([]string, error) {
	return []string{}, nil
}

// resources renders the report index as picker entries, ordered by name.
func (c *Connector) resources() []types.Resource {
	defs := make([]reportDefinition, 0, len(c.reports))
	for _, def := range c.reports {
		defs = append(defs, def)
	}
	sort.Slice(defs, func(i, j int) bool {
		if defs[i].Name != defs[j].Name {
			return defs[i].Name < defs[j].Name
		}
		return defs[i].ID < defs[j].ID
	})

	out := make([]types.Resource, 0, len(defs))
	for _, def := range defs {
		out = append(out, reportResource(def))
	}
	return out
}

// columnsFromDefinition renders a definition's displayFields as picker columns:
// the label the user sees and the "<tableName>_<fieldName>" id the sync uses.
// Both lists are empty for a report that has no displayFields (statistics).
func columnsFromDefinition(def *reportDefinition) ([]string, []string) {
	labels := make([]string, 0, len(def.DisplayFields))
	ids := make([]string, 0, len(def.DisplayFields))
	for _, field := range def.DisplayFields {
		id := fieldID(field.TableName, field.FieldName)
		label := strings.TrimSpace(field.FieldDisplay)
		if label == "" {
			label = id
		}
		labels = append(labels, label)
		ids = append(ids, id)
	}
	return labels, ids
}

// reportResource is the picker's view of one report: flat leaf, one per row.
func reportResource(def reportDefinition) types.Resource {
	description := designTypeLabel(def)
	if def.Category != "" {
		description += " · " + def.Category
	}
	return types.Resource{
		ExternalID:  def.ID,
		Name:        def.Name,
		Type:        "report",
		Description: description,
		Metadata: map[string]interface{}{
			"design_type":      def.DesignType,
			"design_type_name": def.DesignTypeName,
			"category":         def.Category,
			"recommended_api":  def.role(),
			"supports_drill":   def.SupportsDrill,
			"supports_sort":    def.SupportsSort,
		},
	}
}

// batchCollector is the StreamHandler used by the batch entry points. The
// service prefers the streaming path for this connector, so these methods exist
// to satisfy the Connector interface honestly rather than to be the hot path.
type batchCollector struct {
	items []types.FetchedItem
}

func (h *batchCollector) Emit(_ context.Context, item types.FetchedItem) error {
	h.items = append(h.items, item)
	return nil
}

func (h *batchCollector) Checkpoint(context.Context, *types.SyncCursor) error { return nil }

// FetchAll performs a full sync. Every part is re-emitted because a report is a
// live snapshot rather than a document set — there is no cheap way to know the
// data is unchanged without reading it.
func (c *Connector) FetchAll(
	ctx context.Context, ds *types.DataSourceConfig, resourceIDs []string,
) ([]types.FetchedItem, error) {
	handler := &batchCollector{}
	_, err := c.fetch(ctx, ds, resourceIDs, nil, true, handler)
	return handler.items, err
}

// FetchIncremental re-reads the selected reports and re-emits only the parts
// whose content changed.
func (c *Connector) FetchIncremental(
	ctx context.Context, ds *types.DataSourceConfig, cursor *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	handler := &batchCollector{}
	next, err := c.fetch(ctx, ds, ds.ResourceIDs, cursor, false, handler)
	return handler.items, next, err
}

// FetchStream is the production path: parts are emitted as soon as they close,
// so only one part is ever held in memory, and the cursor is checkpointed after
// each completed report.
func (c *Connector) FetchStream(
	ctx context.Context, ds *types.DataSourceConfig,
	cursor *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return c.fetch(ctx, ds, ds.ResourceIDs, cursor, false, h)
}

// FetchFullStream re-fetches every part while keeping the stored cursor as the
// deletion baseline, so a force-full run repairs content without losing the
// table of what the previous sync had written.
func (c *Connector) FetchFullStream(
	ctx context.Context, ds *types.DataSourceConfig,
	cursor *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return c.fetch(ctx, ds, ds.ResourceIDs, cursor, true, h)
}

// fetch is the shared core behind every entry point.
func (c *Connector) fetch(
	ctx context.Context,
	ds *types.DataSourceConfig,
	resourceIDs []string,
	cursor *types.SyncCursor,
	force bool,
	h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	cli, cfg, err := newConfiguredClient(ctx, ds)
	if err != nil {
		return nil, err
	}

	prev, _, decodeErr := decodeCursor(cursor)
	if decodeErr != nil {
		// An undecodable cursor costs one re-emit of unchanged parts, which is
		// always safe; failing here would wedge the data source instead.
		logger.Warnf(ctx, "[Seeyon] ignoring an undecodable sync cursor (parts will be re-synced): %v", decodeErr)
	}

	targets, err := syncTargets(ctx, cli, cfg, ds, resourceIDs)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		// Every selected report was filtered out (deleted upstream, or its
		// design type is no longer enabled). Succeed with an empty cursor
		// rather than failing: the run genuinely has nothing to do, and the
		// warn above names each skipped report.
		logger.Warnf(ctx,
			"[Seeyon] all selected reports were filtered out (configured design types: %s)",
			strings.Join(cfg.DesignTypes, ", "))
	}

	// One client for the whole run: syncReport only needs the resolved tools and
	// the parsed config, never the report index.
	runner := &Connector{client: cli, cfg: cfg}
	next := &syncCursor{Reports: make(map[string]reportCursor, len(targets))}
	now := time.Now().UTC()
	var failures []string

	for _, def := range targets {
		previous := prev.Reports[def.ID]
		reportState, syncErr := runner.syncReport(ctx, def, &previous, force, h)
		if syncErr != nil {
			// A configuration error is not a "this report failed" event: the data
			// source as a whole cannot sync, so it must reach the caller as a
			// configuration error (the UI tells the operator what to fix) rather
			// than as a partial success.
			if errors.Is(syncErr, datasource.ErrInvalidConfig) {
				return next.encode(now), syncErr
			}
			logger.Warnf(ctx, "[Seeyon] report %s (%s) failed: %v", def.Name, def.ID, syncErr)
			failures = append(failures, fmt.Sprintf("报表 %s: %v", def.Name, syncErr))
			// Carry the previous progress forward so a resume retries this
			// report rather than treating it as already handled.
			if existing, ok := prev.Reports[def.ID]; ok {
				next.Reports[def.ID] = existing
			}
			continue
		}
		next.Reports[def.ID] = reportState
		if checkpointErr := h.Checkpoint(ctx, next.encode(now)); checkpointErr != nil {
			return nil, checkpointErr
		}
	}

	out := next.encode(time.Now().UTC())
	if len(failures) > 0 {
		// Partial success: the caller ingests what was emitted and surfaces the
		// per-report reasons, instead of discarding a good subset.
		return out, &datasource.PartialFetchError{Details: failures}
	}
	return out, nil
}

// selectReports resolves the single selected resource id against the report
// index.
//
// Exactly one report is allowed, which is the whole point of the "one data
// source = one report" model: the data source is named after its report
// (Seeyon-<report name>), so it can only carry one. An empty selection is
// rejected rather than read as "everything this data source can see" — reports
// can hold hundreds of thousands of rows, so widening silently would be a very
// large, very surprising job.
func syncTargets(
	ctx context.Context,
	cli *client,
	cfg *config,
	ds *types.DataSourceConfig,
	resourceIDs []string,
) ([]reportDefinition, error) {
	ids := make([]string, 0, len(resourceIDs))
	seen := make(map[string]bool, len(resourceIDs))
	for _, raw := range resourceIDs {
		id := strings.TrimSpace(raw)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf(
			"%w: no report selected; pick one report in the data source",
			datasource.ErrInvalidConfig,
		)
	}
	if len(ids) > 1 {
		return nil, fmt.Errorf(
			"%w: a data source syncs exactly one report, but %d were selected; "+
				"create one data source per report",
			datasource.ErrInvalidConfig, len(ids),
		)
	}
	def, err := resolveReport(ctx, cli, cfg, ids[0], reportNameHint(ds))
	if err != nil {
		if !errors.Is(err, datasource.ErrInvalidConfig) {
			return nil, err
		}
		// The report was deleted upstream, renamed past its saved hint, or its
		// design type is disabled in settings; either way it is not in scope for
		// this run. Returning no targets (instead of an error) keeps the run a
		// no-op rather than a failure for something the operator cannot act on
		// here.
		logger.Warnf(ctx,
			"[Seeyon] selected report %s is not reachable "+
				"(deleted upstream, renamed, or its design type is filtered out): %v; "+
				"nothing to sync", ids[0], err)
		return nil, nil
	}
	return []reportDefinition{def}, nil
}

// rejectIgnoredFilters turns the runtime's "I did not apply this" feedback into
// an actionable configuration error. A silently dropped condition is the worst
// failure mode this connector has: the sync succeeds and reads the *whole*
// report where the operator configured a slice. A dropped sort only
// changes the row order, so it is a warning.
func rejectIgnoredFilters(ctx context.Context, def reportDefinition, page *reportPage) error {
	for _, ignored := range page.IgnoredSorts {
		logger.Warnf(ctx,
			"[Seeyon] report %s: runtime ignored sort %s.%s %s (%s); falling back to its default order",
			def.ID, ignored.TableName, ignored.FieldName, ignored.Order, ignored.Reason)
	}
	var problems []string
	for _, ignored := range page.IgnoredConditions {
		problems = append(problems, fmt.Sprintf(
			"condition %s.%s %s %q (%s)",
			ignored.TableName, ignored.FieldName, ignored.Op, ignored.Value, ignored.Reason))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: report %q rejected %s, so the sync would have read the whole report "+
			"instead of the configured slice; fix the condition (FIELD_NOT_FOUND means the "+
			"field no longer exists, ENUM_VALUE_NOT_FOUND means the value is not one of the "+
			"field's enumOptions)",
		datasource.ErrInvalidConfig, def.Name, strings.Join(problems, "; "))
}

// syncReport fetches one report page by page, emitting one document per closed
// part and finally the overview document.
//
// Ordering is deliberate: parts are emitted before the overview. The overview
// carries ReplacesSubtree plus the complete SubtreeKeep list, and the sweep runs
// when it is ingested, so emitting it last means every part it wants to keep is
// already present. Emitting it first would risk sweeping parts the same sync is
// about to write.
//
// A sync that dies after emitting parts but before the overview leaves those
// parts without a parent, and therefore un-swept. That converges: part external
// ids are derived from the row index, so the next successful run of the same
// report emits the same ids and its overview's SubtreeKeep adopts them, after
// which the normal sweep applies.
func (c *Connector) syncReport(
	ctx context.Context,
	def reportDefinition,
	prev *reportCursor,
	force bool,
	h datasource.StreamHandler,
) (reportCursor, error) {
	role := def.role()
	tool := c.client.tools[role]
	if strings.TrimSpace(tool) == "" {
		return reportCursor{}, fmt.Errorf("%w: no resolved tool for role %s", errToolNotFound, role)
	}
	// The legacy single-tool build dispatches on designType; the report-runtime
	// tools route by tool name and reject an unexpected designType.
	includeDesignType := c.client.legacyTool != "" && tool == c.client.legacyTool

	// The change marker and the snapshot order key live in the report
	// definition, not in the listing index, so they are discovered once and
	// then carried in the cursor: later syncs skip the extra listing call.
	changeField, sortField := prev.ChangeField, prev.SortField
	if changeField == "" || sortField == "" {
		full, inspectErr := c.client.findReportDefinition(
			ctx, c.client.tools[toolRoleReports], def.DesignType, def.ID, def.Name,
		)
		if inspectErr != nil {
			// Not fatal: without the definition we cannot probe or order, so the
			// sync falls back to a full read in the runtime's default order.
			logger.Warnf(ctx, "[Seeyon] cannot inspect report %s definition: %v", def.ID, inspectErr)
		} else {
			if changeField == "" {
				if table, field, ok := changeMarker(*full); ok {
					changeField = table + "." + field
				}
			}
			if sortField == "" {
				if table, field, ok := sortKey(*full); ok {
					sortField = table + "." + field
				}
			}
		}
	}

	syncedAt := time.Now().UTC()
	readStarted := time.Now()

	// Change probe. Two one-row requests answer "how many rows does the
	// filtered report have now" and "how many changed since the watermark". If
	// neither moved, the unneeded full walk is skipped entirely. A full read is
	// forced every full_resync_hours so a source that does not maintain its
	// modify timestamp still converges.
	total := prev.Total
	if c.cfg.ProbeIncremental && !force && changeField != "" && prev.Watermark != "" &&
		time.Since(prev.LastFullSync) < time.Duration(c.cfg.FullResyncHours)*time.Hour {
		current, err := c.probeTotal(ctx, tool, def, includeDesignType)
		if err != nil {
			return reportCursor{}, err
		}
		total = current
		if current != prev.Total {
			logger.Infof(ctx, "[Seeyon] report %s row count %d -> %d; re-reading",
				def.ID, prev.Total, current)
		} else {
			changed, err := c.probeChanged(ctx, tool, def, includeDesignType, changeField, prev.Watermark)
			if err != nil {
				return reportCursor{}, err
			}
			if changed == 0 {
				logger.Infof(ctx,
					"[Seeyon] report %s unchanged since %s; skipping the read", def.ID, prev.Watermark)
				skipped := *prev
				skipped.SyncedAt = time.Now().UTC()
				return skipped, nil
			}
			logger.Infof(ctx, "[Seeyon] report %s: %d rows changed since %s; re-reading",
				def.ID, changed, prev.Watermark)
		}
	}

	// Ordering: ascending keeps part boundaries stable, so an inserted row only
	// rewrites the trailing part instead of shifting every row index (the
	// runtime defaults to newest-first, which would rewrite the entire report on
	// each insert). A truncated read is the exception: there, the newest rows are
	// the ones worth keeping.
	// A fresh cursor carries no usable row count, so assume the read may
	// truncate and keep the newest rows until a complete read records the real
	// total. Getting this wrong the other way would silently sync the oldest
	// max_rows rows of a report with hundreds of thousands of rows and never the new ones.
	knownSize := !prev.SyncedAt.IsZero()
	truncating := c.cfg.MaxRows > 0 && (!knownSize || total > c.cfg.MaxRows)
	sorts := c.snapshotOrder(role, sortField, truncating)
	if c.cfg.Granularity == granularityRow && total > maxRowDocuments {
		return reportCursor{}, rowDocumentLimitError(def, total)
	}

	var (
		renderer     *reportRenderer
		columns      []string
		fieldIDs     []string
		allColumns   []string
		selected     []int
		sourceTotal  = -1
		parts        []partSummary
		hashes       = make(map[string]string)
		oversized    int
		hitRowLimit  bool
		partLimitHit bool
	)

	for page := 1; page <= maxPages; page++ {
		pageData, err := c.client.fetchReportPage(
			ctx, tool, def.ID, def.DesignType, page, c.cfg.PageSize, includeDesignType,
			c.cfg.Conditions, sorts,
		)
		if err != nil {
			return reportCursor{}, err
		}
		if page == 1 {
			// The runtime does not fail on a condition it cannot apply: it drops
			// it, echoes it in ignoredConditions and returns the *unfiltered*
			// rows. Proceeding would silently sync orders of magnitude more data
			// than the operator configured (far more rows than a slice), so a
			// dropped condition is a configuration error, not a warning.
			if err := rejectIgnoredFilters(ctx, def, pageData); err != nil {
				return reportCursor{}, err
			}
			allColumns = pageData.Columns
			columns, fieldIDs, selected = projectColumns(ctx, allColumns, pageData.FieldIDs, c.cfg.Fields)
			sourceTotal = pageData.TotalRecords
			if c.cfg.Granularity == granularityRow && sourceTotal > maxRowDocuments {
				return reportCursor{}, rowDocumentLimitError(def, sourceTotal)
			}
			// The renderer needs the columns to build the Markdown table header,
			// which is only known from the first page.
			if c.cfg.Granularity == granularityRow {
				renderer = newRowRenderer(def.ID, def.Name, columns)
			} else {
				renderer = newReportRenderer(c.cfg.MaxDocumentBytes, def.ID, def.Name, columns)
			}
			renderer.fieldSelection = fieldSelection{
				Selected: len(selected),
				Total:    len(allColumns),
				Filtered: len(c.cfg.Conditions) > 0,
				Fields:   c.cfg.Fields,
			}
		}
		if renderer == nil {
			break
		}

		for i, row := range pageData.Rows {
			row = projectRow(row, selected)
			if c.cfg.MaxRows > 0 && renderer.totalRowsAdded() >= c.cfg.MaxRows {
				hitRowLimit = true
				break
			}
			// Per-row granularity names each document after the runtime's row id
			// so it survives inserts and reorders; statistics rows have no id,
			// which the renderer turns into a positional token.
			token := ""
			if i < len(pageData.RowIDs) {
				token = pageData.RowIDs[i]
			}
			for _, part := range renderer.addRow(row, token) {
				if err := c.emitPart(ctx, h, def, part, prev, force, hashes, &parts, &oversized); err != nil {
					return reportCursor{}, err
				}
			}
		}
		if hitRowLimit {
			break
		}
		if renderer.partsClosed() >= maxPartsPerReport {
			partLimitHit = true
			break
		}
		// A short page means the server has no more rows, whatever the totals
		// claimed; trusting the page contents avoids an endless loop on a
		// server that misreports totalPages.
		if len(pageData.Rows) == 0 {
			break
		}
		if pageData.TotalPages > 0 && page >= pageData.TotalPages {
			break
		}
	}

	if renderer == nil {
		// No page could be read as a report body; treat it as an empty report so
		// the overview still reaches the knowledge base.
		if c.cfg.Granularity == granularityRow {
			renderer = newRowRenderer(def.ID, def.Name, nil)
		} else {
			renderer = newReportRenderer(c.cfg.MaxDocumentBytes, def.ID, def.Name, nil)
		}
	}
	if trailing := renderer.flush(); trailing != nil {
		if err := c.emitPart(ctx, h, def, trailing, prev, force, hashes, &parts, &oversized); err != nil {
			return reportCursor{}, err
		}
	}

	emitted := renderer.totalRowsAdded()
	truncated := hitRowLimit || partLimitHit || (sourceTotal > 0 && emitted < sourceTotal)

	keep := make([]string, 0, len(parts))
	for _, part := range parts {
		keep = append(keep, part.ExternalID)
	}

	overview := buildOverview(overviewInput{
		Definition:     def,
		Columns:        columns,
		FieldIDs:       fieldIDs,
		Fields:         renderer.fieldSelection,
		Parts:          parts,
		SourceTotal:    sourceTotal,
		EmittedRows:    emitted,
		Truncated:      truncated,
		PartLimitHit:   partLimitHit,
		OversizedParts: oversized,
		SyncedAt:       syncedAt,
	})

	metadata := reportMetadata(def, syncedAt)
	metadata["report_total_rows"] = strconv.Itoa(maxInt(sourceTotal, 0))
	metadata["report_rows"] = strconv.Itoa(emitted)
	metadata["report_truncated"] = strconv.FormatBool(truncated)
	metadata["report_parts"] = strconv.Itoa(len(parts))
	metadata["report_fields"] = strconv.Itoa(len(columns))
	metadata["report_fields_total"] = strconv.Itoa(len(allColumns))
	metadata["report_conditions"] = strconv.Itoa(len(c.cfg.Conditions))

	parent := types.FetchedItem{
		ExternalID:       def.ID,
		Title:            def.Name,
		Content:          []byte(overview),
		ContentType:      "text/markdown",
		FileName:         reportFileName(def.Name, ""),
		UpdatedAt:        syncedAt,
		CreatedAt:        syncedAt,
		SourceResourceID: def.ID,
		Metadata:         metadata,
		// Drives the stale-part sweep: every part emitted above is listed in
		// SubtreeKeep, so only parts from an earlier sync that no longer exist
		// are deleted.
		ReplacesSubtree: true,
		SubtreeKeep:     keep,
	}
	if err := h.Emit(ctx, parent); err != nil {
		return reportCursor{}, err
	}

	next := reportCursor{
		Parts:       hashes,
		Rows:        emitted,
		Total:       maxInt(sourceTotal, 0),
		Truncated:   truncated,
		SyncedAt:    syncedAt,
		ChangeField: changeField,
		SortField:   sortField,
	}
	// Only a complete read may advance the watermark and the full-sync clock: a
	// truncated read never covered the tail, so marking it as seen would make
	// the next probe skip exactly the rows it never fetched.
	if truncated {
		next.Watermark = prev.Watermark
		next.LastFullSync = prev.LastFullSync
	} else {
		next.Watermark = c.watermark(readStarted)
		next.LastFullSync = readStarted.UTC()
	}
	return next, nil
}

// probeTotal reports how many rows the runtime sees for the configured filters.
// It asks for a single row: the count comes back in totalRecords either way, so
// a probe costs one row instead of 684 pages.
func (c *Connector) probeTotal(
	ctx context.Context, tool string, def reportDefinition, includeDesignType bool,
) (int, error) {
	page, err := c.client.fetchReportPage(
		ctx, tool, def.ID, def.DesignType, 1, 1, includeDesignType, c.cfg.Conditions, nil,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectIgnoredFilters(ctx, def, page); err != nil {
		return 0, err
	}
	return page.TotalRecords, nil
}

// probeChanged counts the rows modified after the watermark. The marker is the
// CAP4 modify_date system field, which is filterable but never displayed.
func (c *Connector) probeChanged(
	ctx context.Context, tool string, def reportDefinition, includeDesignType bool,
	changeField, watermark string,
) (int, error) {
	table, field, ok := splitFieldRef(changeField)
	if !ok {
		return 0, nil
	}
	conditions := make([]reportCondition, 0, len(c.cfg.Conditions)+1)
	conditions = append(conditions, c.cfg.Conditions...)
	conditions = append(conditions, reportCondition{
		TableName: table, FieldName: field, Op: "Greater", Value: watermark,
	})
	page, err := c.client.fetchReportPage(
		ctx, tool, def.ID, def.DesignType, 1, 1, includeDesignType, conditions, nil,
	)
	if err != nil {
		return 0, err
	}
	if err := rejectIgnoredFilters(ctx, def, page); err != nil {
		return 0, err
	}
	return page.TotalRecords, nil
}

// snapshotOrder picks the request order. Ascending keeps the snapshot's part
// boundaries stable; a truncated read stays newest-first so the rows that are
// kept are the recent ones. The statistics tool has no sorts parameter.
func (c *Connector) snapshotOrder(role, sortField string, truncating bool) []reportSort {
	if role != toolRoleQuery || sortField == "" {
		return nil
	}
	table, field, ok := splitFieldRef(sortField)
	if !ok {
		return nil
	}
	order := "asc"
	if truncating {
		order = "desc"
	}
	return []reportSort{{TableName: table, FieldName: field, Order: order}}
}

// watermark renders the probe lower bound in the gateway's wall-clock time. The
// offset exists because the gateway's report timestamps are the OA server's
// local time, which is not necessarily the sync host's.
func (c *Connector) watermark(at time.Time) string {
	location := c.cfg.TimestampLocation
	if location == nil {
		// A zero-value config in a test: fall back to the deployment default
		// rather than panicking on a nil location pointer.
		if loaded, err := time.LoadLocation(defaultTimestampTimezone); err == nil {
			location = loaded
		}
	}
	if location == nil {
		return at.Format("2006-01-02 15:04:05")
	}
	return at.In(location).Format("2006-01-02 15:04:05")
}

// splitFieldRef splits a "table.field" reference at the last dot; table names
// contain underscores but not dots.
func splitFieldRef(ref string) (table, field string, ok bool) {
	idx := strings.LastIndex(ref, ".")
	if idx <= 0 || idx == len(ref)-1 {
		return "", "", false
	}
	return ref[:idx], ref[idx+1:], true
}

// rowDocumentLimitError explains why per-row granularity is refused for a large
// report instead of emitting hundreds of thousands of documents.
func rowDocumentLimitError(def reportDefinition, rows int) error {
	return fmt.Errorf(
		"%w: settings.document_granularity=row makes one document per row, but report %q "+
			"has %d rows (limit %d); add fixed filter conditions to narrow the report, or "+
			"switch back to document_granularity=part",
		datasource.ErrInvalidConfig, def.Name, rows, maxRowDocuments)
}

// reportFileName puts every document of a report into a folder named after it.
// The folder is not a separate API call: the sync path derives it from the
// relative file name (types.SplitKnowledgeRelativePath), which keeps one report's
// overview and parts together in a knowledge base shared with other sources.
func reportFileName(reportName, suffix string) string {
	folder := strings.TrimSpace(reportName)
	name := folder
	if suffix != "" {
		name = folder + " " + suffix
	}
	name = datasource.SanitizeFileName(name)
	if folder == "" {
		return name + ".md"
	}
	return datasource.SanitizeFileName(folder) + "/" + name + ".md"
}

// emitPart records a closed part in the cursor and emits it unless its content
// is byte-for-byte what the previous sync already wrote.
func (c *Connector) emitPart(
	ctx context.Context,
	h datasource.StreamHandler,
	def reportDefinition,
	part *renderedPart,
	prev *reportCursor,
	force bool,
	hashes map[string]string,
	parts *[]partSummary,
	oversized *int,
) error {
	sum := sha256.Sum256(part.Content)
	digest := hex.EncodeToString(sum[:])

	hashes[part.ExternalID] = digest
	*parts = append(*parts, partSummary{
		ExternalID: part.ExternalID,
		StartRow:   part.StartRow,
		EndRow:     part.EndRow,
	})
	if part.Oversized {
		*oversized++
		logger.Warnf(ctx,
			"[Seeyon] report %s part %d exceeds max_document_bytes because a single row is larger; "+
				"the part is emitted anyway so no data is dropped",
			def.ID, part.Index)
	}

	if !force && prev != nil && prev.Parts[part.ExternalID] == digest {
		// Unchanged: keep it in SubtreeKeep (already recorded above) so the
		// sweep preserves the existing document, but skip the re-ingest.
		return nil
	}

	metadata := reportMetadata(def, time.Now().UTC())
	metadata["part_index"] = strconv.Itoa(part.Index)
	metadata["row_start"] = strconv.Itoa(part.StartRow)
	metadata["row_end"] = strconv.Itoa(part.EndRow)
	metadata["row_count"] = strconv.Itoa(part.Rows)
	if part.Oversized {
		metadata["oversized"] = "true"
	}

	title := fmt.Sprintf("%s（第 %d–%d 行）", def.Name, part.StartRow, part.EndRow)
	if part.Rows == 1 && part.ExternalID != "" && strings.Contains(part.ExternalID, "#row#") {
		title = fmt.Sprintf("%s（单条记录）", def.Name)
	}
	item := types.FetchedItem{
		ExternalID:       part.ExternalID,
		Title:            title,
		Content:          part.Content,
		ContentType:      "text/markdown",
		FileName:         reportFileName(def.Name, fmt.Sprintf("%04d", part.Index)),
		UpdatedAt:        time.Now().UTC(),
		CreatedAt:        time.Now().UTC(),
		SourceResourceID: def.ID,
		Metadata:         metadata,
	}
	return h.Emit(ctx, item)
}

// reportMetadata is the metadata shared by a report's overview and parts.
func reportMetadata(def reportDefinition, syncedAt time.Time) map[string]string {
	return map[string]string{
		"channel":          types.ConnectorTypeSeeyon,
		"seeyon_report_id": def.ID,
		"report_name":      def.Name,
		"design_type":      def.DesignType,
		"synced_at":        syncedAt.UTC().Format(time.RFC3339),
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
