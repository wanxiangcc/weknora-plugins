package svn

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

// maxSupportedFileBytes is the hard ceiling for the per-file limit. Ingesting a
// file costs roughly three times its size in peak RSS (connector buffer ->
// multipart copy -> parser request), so a larger value risks OOM-killing the
// whole service instead of skipping one oversized file.
const maxSupportedFileBytes int64 = 1024 << 20

// repository is one SVN repository (or subdirectory of one) declared by the
// user. The connector talks WebDAV, which only exists on http(s):// URLs —
// mod_dav_svn behind Apache/VisualSVN. svn://, svn+ssh:// and file:// are
// rejected at config time with a message that says so instead of failing
// later with an opaque transport error.
type repository struct {
	// URL is the absolute http(s) URL of the repository root or of any
	// subdirectory below it, without a trailing slash.
	URL string `json:"url"`
	// Paths narrows the sync to these repository-relative directories.
	// Empty means the whole URL subtree.
	Paths []string `json:"paths"`
}

// config is the connector-specific view of a data source configuration.
//
// base_url is the SVN URL the user browses (a repository root or the collection
// that contains them). It is what the connection test probes and what the sync
// falls back to when no repository list is configured. repositories holds the
// actual sync targets: extra repositories, and/or directory filters per
// repository.
type config struct {
	BaseURL      string
	Repositories []repository
	Username     string
	Password     string
	// MaxFileBytes is the largest repository file this data source will fetch.
	// It is never larger than the deployment's own ingest ceiling, so the sync
	// does not spend bandwidth on a file the platform cannot ingest.
	MaxFileBytes int64
}

// resolveMaxFileBytes decides how large a repository file may be.
//
// The ceiling is the deployment's own ingest limit (MAX_FILE_SIZE_MB, the same
// value that sizes the docreader gRPC frame), because fetching more than the
// platform can ingest only wastes bandwidth and floods the sync log with parse
// failures. settings.max_file_size_mb may narrow that limit further — for
// example to keep a repository's legacy media exports out of the knowledge
// base. Raising it needs the deployment limit raised first; an attempt to go
// above it is clamped (with a warning) rather than rejected, so lowering
// MAX_FILE_SIZE_MB in a running deployment cannot break existing data sources.
func resolveMaxFileBytes(raw interface{}) int64 {
	limit := utils.GetMaxFileSize()
	if mb, ok := settingMegabytes(raw); ok {
		requested := mb * 1024 * 1024
		if requested < limit {
			limit = requested
		} else if requested > limit {
			logger.Warnf(context.Background(),
				"[SVN] settings.max_file_size_mb=%d is above the deployment ingest limit %d MB; "+
					"raise MAX_FILE_SIZE_MB (app, docreader and nginx) to sync larger files",
				mb, limit/(1024*1024))
		}
	} else if raw != nil {
		logger.Warnf(context.Background(),
			"[SVN] ignoring settings.max_file_size_mb=%v: expected a positive whole number of megabytes", raw)
	}
	if limit > maxSupportedFileBytes {
		logger.Warnf(context.Background(),
			"[SVN] file size limit %d MB exceeds the supported ceiling %d MB; using the ceiling. "+
				"Each ingested file costs about three times its size in memory",
			limit/(1024*1024), maxSupportedFileBytes/(1024*1024))
		limit = maxSupportedFileBytes
	}
	return limit
}

// settingMegabytes reads a positive whole number of megabytes out of the
// untyped settings map (JSON numbers decode to float64, but a cursor round trip
// or a hand-written config can produce other shapes).
func settingMegabytes(raw interface{}) (int64, bool) {
	var value float64
	switch typed := raw.(type) {
	case float64:
		value = typed
	case float32:
		value = float64(typed)
	case int:
		value = float64(typed)
	case int64:
		value = float64(typed)
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return 0, false
		}
		value = parsed
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		value = parsed
	default:
		return 0, false
	}
	if value <= 0 || value != math.Trunc(value) {
		return 0, false
	}
	if value > math.MaxInt64/float64(1<<20) {
		return 0, false
	}
	return int64(value), true
}

func parseConfig(ds *types.DataSourceConfig) (*config, error) {
	if ds == nil {
		return nil, datasource.ErrInvalidConfig
	}

	rawBase, _ := ds.Credentials["base_url"].(string)
	baseURL, err := normalizeRepositoryURL(rawBase)
	if err != nil {
		return nil, err
	}

	out := &config{BaseURL: baseURL}
	out.MaxFileBytes = resolveMaxFileBytes(ds.Settings["max_file_size_mb"])
	if raw, ok := ds.Settings["repositories"]; ok {
		items, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("%w: settings.repositories must be an array", datasource.ErrInvalidConfig)
		}
		// The same repository may legitimately appear twice with different
		// paths, but a duplicate URL would otherwise silently double-sync.
		// Merge duplicates by unioning their path selections.
		byURL := map[string]int{}
		for _, rawRepo := range items {
			m, ok := rawRepo.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("%w: invalid repository entry", datasource.ErrInvalidConfig)
			}
			rawURL, _ := m["url"].(string)
			repoURL, err := normalizeRepositoryURL(rawURL)
			if err != nil {
				return nil, err
			}
			var paths []string
			if rawPaths, exists := m["paths"]; exists {
				values, ok := rawPaths.([]interface{})
				if !ok {
					return nil, fmt.Errorf("%w: paths must be an array", datasource.ErrInvalidConfig)
				}
				for _, value := range values {
					s, ok := value.(string)
					if !ok {
						return nil, fmt.Errorf("%w: path must be a string", datasource.ErrInvalidConfig)
					}
					normalized, err := normalizePath(s)
					if err != nil {
						return nil, err
					}
					paths = append(paths, normalized)
				}
			}
			paths = collapsePaths(paths)
			if idx, dup := byURL[repoURL]; dup {
				out.Repositories[idx].Paths = collapsePaths(
					append(out.Repositories[idx].Paths, paths...),
				)
				continue
			}
			byURL[repoURL] = len(out.Repositories)
			out.Repositories = append(out.Repositories, repository{URL: repoURL, Paths: paths})
		}
	}
	// No repository list (or an empty one) means "sync base_url itself", so a
	// user who only filled in the connection fields still gets a working sync.
	if len(out.Repositories) == 0 {
		out.Repositories = []repository{{URL: baseURL}}
	}

	if username, ok := ds.Credentials["username"].(string); ok {
		out.Username = strings.TrimSpace(username)
	}
	if password, ok := ds.Credentials["password"].(string); ok {
		out.Password = password
	}
	if out.Password != "" && out.Username == "" {
		return nil, fmt.Errorf("%w: password requires a username", datasource.ErrInvalidCredentials)
	}
	return out, nil
}

// normalizeRepositoryURL validates and canonicalises one SVN URL. It enforces the
// http(s) scheme (all the connector can speak) and runs the shared connector
// SSRF policy, so an operator must whitelist an internal SVN host the same way
// they whitelist an internal GitLab instance.
func normalizeRepositoryURL(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", fmt.Errorf("%w: SVN url is required", datasource.ErrInvalidConfig)
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("%w: invalid SVN url %q", datasource.ErrInvalidConfig, raw)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf(
			"%w: SVN url must use http(s); %q is not reachable over WebDAV "+
				"(svn:// and svn+ssh:// are not supported)",
			datasource.ErrInvalidConfig, parsed.Scheme,
		)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%w: SVN url has no host", datasource.ErrInvalidConfig)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%w: SVN url must not carry a query or fragment", datasource.ErrInvalidConfig)
	}
	if parsed.User != nil {
		return "", fmt.Errorf(
			"%w: SVN url must not embed credentials; use the username/password fields",
			datasource.ErrInvalidConfig,
		)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	canonical := parsed.String()
	if err := datasource.ValidateConnectorBaseURL(canonical); err != nil {
		return "", err
	}
	return canonical, nil
}

// normalizePath validates a repository-relative directory selection.
func normalizePath(value string) (string, error) {
	v := strings.Trim(strings.TrimSpace(value), "/")
	if v == "" {
		return "", nil
	}
	if strings.Contains(v, "\\") {
		return "", fmt.Errorf("%w: path must use forward slashes", datasource.ErrInvalidConfig)
	}
	if path.Clean(v) != v || v == "." || strings.HasPrefix(v, "../") || strings.Contains(v, "/../") {
		return "", fmt.Errorf("%w: invalid repository path", datasource.ErrInvalidConfig)
	}
	return v, nil
}

// collapsePaths sorts selections, drops nested duplicates and treats an empty
// selection (the repository root) as covering everything.
func collapsePaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	sort.Strings(paths)
	for _, p := range paths {
		if p == "" {
			return nil
		}
	}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if len(out) > 0 && (p == out[len(out)-1] || strings.HasPrefix(p, out[len(out)-1]+"/")) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// repositoryName derives the display name used for titles and the KB folder
// layout. The last non-empty URL segment is the conventional SVN repository
// name; hosts serving a repository from "/" fall back to the host name.
func repositoryName(repoURL string) string {
	parsed, err := url.Parse(repoURL)
	if err != nil {
		return "svn"
	}
	if name := path.Base(strings.TrimRight(parsed.Path, "/")); name != "" && name != "." && name != "/" {
		return name
	}
	if parsed.Host != "" {
		return parsed.Host
	}
	return "svn"
}

// directoryURL joins a repository-relative directory selection onto a
// repository URL, percent-encoding each segment so spaces and non-ASCII names
// survive the round trip.
func directoryURL(repoURL, dir string) string {
	if dir == "" {
		return repoURL
	}
	segments := strings.Split(dir, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	return repoURL + "/" + strings.Join(segments, "/")
}

// svnExternalID is the stable knowledge identity of one repository file. It is
// deliberately revision-free so that a later revision of the same path updates
// the existing knowledge entry instead of creating a second one.
func svnExternalID(repoURL, idPath string) string {
	return "svn:" + repoURL + ":" + idPath
}
