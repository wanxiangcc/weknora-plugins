package seeyon

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// fixedSyncTime keeps cursor assertions deterministic.
var fixedSyncTime = time.Date(2026, 10, 16, 9, 40, 51, 0, time.UTC)

func TestParseConfigAppliesDocumentedDefaults(t *testing.T) {
	cfg, err := parseConfig(&types.DataSourceConfig{
		Credentials: map[string]interface{}{
			"base_url":     testGatewayURL,
			"access_token": "  " + testToken + "  ",
		},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.BaseURL != testGatewayURL {
		t.Fatalf("base_url must be used verbatim, got %q", cfg.BaseURL)
	}
	if cfg.AccessToken != testToken {
		t.Fatalf("the access token must be trimmed, got %q", cfg.AccessToken)
	}
	if cfg.PageSize != defaultPageSize {
		t.Fatalf("page_size default = %d, want %d", cfg.PageSize, defaultPageSize)
	}
	if cfg.MaxDocumentBytes != defaultMaxDocumentBytes {
		t.Fatalf("max_document_bytes default = %d, want %d", cfg.MaxDocumentBytes, defaultMaxDocumentBytes)
	}
	if cfg.MaxRows != defaultMaxRows {
		t.Fatalf("max_rows default = %d, want %d", cfg.MaxRows, defaultMaxRows)
	}
	if !reflect.DeepEqual(cfg.DesignTypes, defaultDesignTypes) {
		t.Fatalf("design_types default = %v, want %v", cfg.DesignTypes, defaultDesignTypes)
	}
	if len(cfg.ToolOverrides) != 0 {
		t.Fatalf("tool_overrides must default to empty, got %v", cfg.ToolOverrides)
	}
}

func TestParseConfigRejectsMissingCredentials(t *testing.T) {
	cases := []struct {
		name        string
		credentials map[string]interface{}
		wantText    string
	}{
		{
			name:        "no base url",
			credentials: map[string]interface{}{"access_token": testToken},
			wantText:    "credentials.base_url is required",
		},
		{
			name:        "no access token",
			credentials: map[string]interface{}{"base_url": testGatewayURL},
			wantText:    "credentials.access_token is required",
		},
		{
			name: "blank access token",
			credentials: map[string]interface{}{
				"base_url":     testGatewayURL,
				"access_token": "   ",
			},
			wantText: "credentials.access_token is required",
		},
		{
			name: "non-string access token",
			credentials: map[string]interface{}{
				"base_url":     testGatewayURL,
				"access_token": 12345,
			},
			wantText: "credentials.access_token is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig(&types.DataSourceConfig{Credentials: tc.credentials})
			if !errors.Is(err, datasource.ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q must mention %q", err, tc.wantText)
			}
		})
	}
}

func TestParseConfigRejectsBadBaseURL(t *testing.T) {
	// svn:// is not an http(s) endpoint, so the SSRF guard must reject it before
	// any request is attempted.
	_, err := parseConfig(&types.DataSourceConfig{
		Credentials: map[string]interface{}{
			"base_url":     "svn://example.com/repo",
			"access_token": testToken,
		},
	})
	if !errors.Is(err, datasource.ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig for a non-http scheme, got %v", err)
	}
}

func TestParseConfigDesignTypes(t *testing.T) {
	cases := []struct {
		name      string
		raw       interface{}
		want      []string
		wantError string
	}{
		{
			name: "explicit subset",
			raw:  []interface{}{"QUERY", "STATISTICS"},
			want: []string{"QUERY", "STATISTICS"},
		},
		{
			name: "lower case and padding are normalized",
			raw:  []interface{}{" query ", "statistics"},
			want: []string{"QUERY", "STATISTICS"},
		},
		{
			name: "duplicates collapse",
			raw:  []interface{}{"QUERY", "QUERY"},
			want: []string{"QUERY"},
		},
		{
			name: "empty array means all syncable types",
			raw:  []interface{}{},
			want: defaultDesignTypes,
		},
		{
			name: "the two syncable types",
			raw:  []interface{}{"QUERY", "STATISTICS"},
			want: []string{"QUERY", "STATISTICS"},
		},
		{
			name:      "a known but unsupported type is rejected with its own reason",
			raw:       []interface{}{"MULTSTATS"},
			wantError: "not syncable",
		},
		{
			name:      "unknown type is rejected rather than ignored",
			raw:       []interface{}{"QUERY", "NOT_A_REPORT"},
			wantError: "unknown type",
		},
		{
			name:      "SUMMARY is recognized but not syncable",
			raw:       []interface{}{"SUMMARY"},
			wantError: "not syncable",
		},
		{
			name:      "a non-array is rejected",
			raw:       "QUERY",
			wantError: "must be an array",
		},
		{
			name:      "non-string entries are rejected",
			raw:       []interface{}{"QUERY", 7},
			wantError: "only strings",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDesignTypes(tc.raw)
			if tc.wantError != "" {
				if err == nil {
					t.Fatalf("want an error mentioning %q, got types %v", tc.wantError, got)
				}
				if !errors.Is(err, datasource.ErrInvalidConfig) {
					t.Fatalf("want ErrInvalidConfig, got %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error %q must mention %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDesignTypes: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("design types = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseConfigNumericSettings(t *testing.T) {
	cases := []struct {
		name         string
		settings     map[string]interface{}
		wantPageSize int
		wantBytes    int
		wantMaxRows  int
		wantError    string
	}{
		{
			name:         "values are taken as given",
			settings:     map[string]interface{}{"page_size": float64(100), "max_document_bytes": float64(262144), "max_rows": float64(10)},
			wantPageSize: 100,
			wantBytes:    262144,
			wantMaxRows:  10,
		},
		{
			name:         "page_size above the CAP4 ceiling is clamped",
			settings:     map[string]interface{}{"page_size": float64(5000)},
			wantPageSize: maxPageSize,
			wantBytes:    defaultMaxDocumentBytes,
			wantMaxRows:  defaultMaxRows,
		},
		{
			name:         "max_rows above the ceiling is clamped",
			settings:     map[string]interface{}{"max_rows": float64(9999999)},
			wantPageSize: defaultPageSize,
			wantBytes:    defaultMaxDocumentBytes,
			wantMaxRows:  maxMaxRows,
		},
		{
			name:         "max_rows zero means unlimited",
			settings:     map[string]interface{}{"max_rows": float64(0)},
			wantPageSize: defaultPageSize,
			wantBytes:    defaultMaxDocumentBytes,
			wantMaxRows:  0,
		},
		{
			name:         "string encoded numbers are accepted",
			settings:     map[string]interface{}{"page_size": "200", "max_document_bytes": "131072", "max_rows": "5"},
			wantPageSize: 200,
			wantBytes:    131072,
			wantMaxRows:  5,
		},
		{
			name:      "page_size below one is rejected",
			settings:  map[string]interface{}{"page_size": float64(0)},
			wantError: "page_size must be a positive whole number",
		},
		{
			name:      "fractional page_size is rejected",
			settings:  map[string]interface{}{"page_size": float64(1.5)},
			wantError: "page_size must be a positive whole number",
		},
		{
			name:      "too small a document is rejected",
			settings:  map[string]interface{}{"max_document_bytes": float64(1024)},
			wantError: "max_document_bytes must be between",
		},
		{
			name:      "too large a document is rejected",
			settings:  map[string]interface{}{"max_document_bytes": float64(1 << 30)},
			wantError: "max_document_bytes must be between",
		},
		{
			name:      "negative max_rows is rejected",
			settings:  map[string]interface{}{"max_rows": float64(-1)},
			wantError: "max_rows must be zero or a positive whole number",
		},
		{
			name:      "unparseable max_rows is rejected",
			settings:  map[string]interface{}{"max_rows": "many"},
			wantError: "max_rows must be zero or a positive whole number",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig(&types.DataSourceConfig{
				Credentials: map[string]interface{}{
					"base_url":     testGatewayURL,
					"access_token": testToken,
				},
				Settings: tc.settings,
			})
			if tc.wantError != "" {
				if err == nil {
					t.Fatalf("want an error mentioning %q", tc.wantError)
				}
				if !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("error %q must mention %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			if cfg.PageSize != tc.wantPageSize {
				t.Fatalf("page_size = %d, want %d", cfg.PageSize, tc.wantPageSize)
			}
			if cfg.MaxDocumentBytes != tc.wantBytes {
				t.Fatalf("max_document_bytes = %d, want %d", cfg.MaxDocumentBytes, tc.wantBytes)
			}
			if cfg.MaxRows != tc.wantMaxRows {
				t.Fatalf("max_rows = %d, want %d", cfg.MaxRows, tc.wantMaxRows)
			}
		})
	}
}

func TestParseConfigToolOverrides(t *testing.T) {
	cfg, err := parseConfig(&types.DataSourceConfig{
		Credentials: map[string]interface{}{
			"base_url":     testGatewayURL,
			"access_token": testToken,
		},
		Settings: map[string]interface{}{
			"tool_overrides": map[string]interface{}{
				"reports":    " pinned-reports ",
				"STATISTICS": "pinned-stats",
			},
		},
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := map[string]string{"reports": "pinned-reports", "statistics": "pinned-stats"}
	if !reflect.DeepEqual(cfg.ToolOverrides, want) {
		t.Fatalf("tool_overrides = %v, want %v", cfg.ToolOverrides, want)
	}
}

func TestParseConfigRejectsBadToolOverrides(t *testing.T) {
	cases := []struct {
		name      string
		raw       interface{}
		wantError string
	}{
		{
			name:      "unknown role",
			raw:       map[string]interface{}{"querys": "x"},
			wantError: "unknown role",
		},
		{
			name:      "blank name",
			raw:       map[string]interface{}{"query": "  "},
			wantError: "must not be blank",
		},
		{
			name:      "non-string name",
			raw:       map[string]interface{}{"query": float64(3)},
			wantError: "must be a string",
		},
		{
			name:      "not an object",
			raw:       []interface{}{"query"},
			wantError: "must be an object",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseToolOverrides(tc.raw)
			if err == nil {
				t.Fatalf("want an error mentioning %q", tc.wantError)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("error %q must mention %q", err, tc.wantError)
			}
		})
	}
}

func TestAPIRoleRoutesStatisticsTypes(t *testing.T) {
	// Only 单表统计 uses the statistics tool; everything else in scope is a query
	// report. The unsupported design types are rejected at configuration time
	// (see parseDesignTypes), so they are not routed here at all.
	if got := apiRole(" statistics "); got != toolRoleStatistics {
		t.Fatalf("apiRole(STATISTICS) = %q, want %q", got, toolRoleStatistics)
	}
	for _, designType := range []string{"QUERY", "", "unknown"} {
		if got := apiRole(designType); got != toolRoleQuery {
			t.Fatalf("apiRole(%q) = %q, want %q", designType, got, toolRoleQuery)
		}
	}
}

func TestReportDefinitionRolePrefersRecommendedAPI(t *testing.T) {
	// The gateway's recommendedApi wins over the design type, because it is the
	// server's own routing decision.
	def := reportDefinition{DesignType: "QUERY", RecommendedAPI: "statistics"}
	if got := def.role(); got != toolRoleStatistics {
		t.Fatalf("role() = %q, want %q", got, toolRoleStatistics)
	}
	// Unknown or absent recommendations fall back to the design type.
	def = reportDefinition{DesignType: "STATISTICS", RecommendedAPI: "nonsense"}
	if got := def.role(); got != toolRoleStatistics {
		t.Fatalf("role() = %q, want %q", got, toolRoleStatistics)
	}
}

func TestSettingIntAcceptsTheShapesThatReachIt(t *testing.T) {
	cases := []struct {
		raw   interface{}
		want  int
		valid bool
	}{
		{raw: float64(7), want: 7, valid: true},
		{raw: float32(7), want: 7, valid: true},
		{raw: 7, want: 7, valid: true},
		{raw: int32(7), want: 7, valid: true},
		{raw: int64(7), want: 7, valid: true},
		{raw: json.Number("7"), want: 7, valid: true},
		{raw: " 7 ", want: 7, valid: true},
		{raw: float64(7.5), valid: false},
		{raw: "seven", valid: false},
		{raw: "", valid: false},
		{raw: nil, valid: false},
		{raw: true, valid: false},
	}
	for _, tc := range cases {
		got, ok := settingInt(tc.raw)
		if ok != tc.valid {
			t.Fatalf("settingInt(%#v) valid = %v, want %v", tc.raw, ok, tc.valid)
		}
		if ok && got != tc.want {
			t.Fatalf("settingInt(%#v) = %d, want %d", tc.raw, got, tc.want)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	original := &syncCursor{Reports: map[string]reportCursor{
		"1001": {
			Parts:     map[string]string{"1001#part#0001": "abc"},
			Rows:      42,
			Total:     42,
			Truncated: true,
		},
	}}
	encoded := original.encode(fixedSyncTime)

	decoded, raw, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("the raw cursor must be retained for diagnostics")
	}
	got := decoded.Reports["1001"]
	if got.Rows != 42 || got.Total != 42 || !got.Truncated {
		t.Fatalf("round trip lost progress: %+v", got)
	}
	if got.Parts["1001#part#0001"] != "abc" {
		t.Fatalf("round trip lost part digests: %+v", got.Parts)
	}
}

func TestDecodeCursorToleratesGarbage(t *testing.T) {
	// A malformed cursor must degrade to "re-sync everything", never to a hard
	// failure that would wedge the data source.
	decoded, raw, err := decodeCursor(&types.SyncCursor{
		ConnectorCursor: map[string]interface{}{"reports": "not-an-object"},
	})
	if err == nil {
		t.Fatal("want a decode error to be reported for diagnostics")
	}
	if decoded == nil || len(decoded.Reports) != 0 {
		t.Fatalf("a malformed cursor must decode to an empty state, got %+v", decoded)
	}
	if len(raw) == 0 {
		t.Fatal("the offending raw cursor must be returned for the warning log")
	}

	decoded, raw, err = decodeCursor(nil)
	if err != nil || raw != nil || decoded == nil || len(decoded.Reports) != 0 {
		t.Fatalf("a nil cursor must decode to an empty state, got %+v / %v / %s", decoded, err, raw)
	}
}
