package svn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

// The SVN connector speaks WebDAV to mod_dav_svn (Apache, VisualSVN, …). It
// deliberately does not shell out to the svn CLI: the backend container ships no
// subversion binary, and spawning one per file would tie the sync to the
// deployment image. The trade-off is that only http(s):// repositories are
// reachable — svn:// and svn+ssh:// are rejected at configuration time.
//
// Incremental sync is snapshot based rather than revision-graph based, because
// plain WebDAV exposes each node's last-changed revision (DAV:version-name) but
// not svn log. Each run walks the selected subtrees once, compares every file's
// revision against the cursor, and downloads only the files that changed.
var (
	_ datasource.Connector              = (*Connector)(nil)
	_ datasource.StreamingConnector     = (*Connector)(nil)
	_ datasource.FullStreamingConnector = (*Connector)(nil)
)

const (
	// maxWalkDepth bounds directory recursion so a repository with a symlink
	// cycle (or a hostile server answering PROPFIND with itself) cannot make the
	// walk unbounded.
	maxWalkDepth = 64
	// maxWalkEntries bounds the number of nodes visited in one repository walk.
	// Beyond this the operator is asked to narrow the selected paths.
	maxWalkEntries = 200000
)

type Connector struct {
	client *client
}

// NewConnector creates a stateless connector. Every data source carries its own
// repository URLs and credentials.
func NewConnector() *Connector { return &Connector{} }

func (c *Connector) Type() string { return types.ConnectorTypeSVN }

func (c *Connector) configured(ds *types.DataSourceConfig) (*Connector, *config, error) {
	cfg, err := parseConfig(ds)
	if err != nil {
		return nil, nil, err
	}
	return &Connector{client: newClient(cfg.Username, cfg.Password, cfg.MaxFileBytes)}, cfg, nil
}

// Validate checks the configuration and proves the credentials can reach the
// configured URL(s) with a Depth: 0 PROPFIND. base_url is always probed, so the
// dialog's connection test works before any repository row is filled in.
func (c *Connector) Validate(ctx context.Context, ds *types.DataSourceConfig) error {
	configured, cfg, err := c.configured(ds)
	if err != nil {
		return err
	}
	targets := []string{cfg.BaseURL}
	for _, repo := range cfg.Repositories {
		if repo.URL != cfg.BaseURL {
			targets = append(targets, repo.URL)
		}
	}
	for _, target := range targets {
		if _, err := configured.client.propfind(ctx, target, "0"); err != nil {
			if isNotImplemented(err) {
				return fmt.Errorf(
					"SVN url %s did not accept WebDAV PROPFIND (%w); "+
						"the URL must be served by mod_dav_svn over http(s)",
					target, err,
				)
			}
			return fmt.Errorf("SVN url %s: %w", target, err)
		}
	}
	return nil
}

// ListResources lists the configured repositories at the root, then one level of
// directories per call. Only directories are returned: selecting a directory
// syncs every supported file below it, mirroring the GitLab connector's picker.
func (c *Connector) ListResources(
	ctx context.Context, ds *types.DataSourceConfig, parent string,
) ([]types.Resource, error) {
	configured, cfg, err := c.configured(ds)
	if err != nil {
		return nil, err
	}
	parent = strings.TrimRight(strings.TrimSpace(parent), "/")
	if parent == "" {
		out := make([]types.Resource, 0, len(cfg.Repositories))
		for _, repo := range cfg.Repositories {
			out = append(out, types.Resource{
				ExternalID:  repo.URL,
				Name:        repositoryName(repo.URL),
				Type:        "repository",
				URL:         repo.URL,
				HasChildren: true,
				Metadata:    map[string]interface{}{"total_paths": len(repo.Paths)},
			})
		}
		return out, nil
	}
	// The picker can only ever hand back URLs this data source declared; anything
	// else would turn the listing endpoint into an SSRF probe.
	if !belongsToConfiguredRepository(cfg, parent) {
		return nil, fmt.Errorf("%w: %s", datasource.ErrResourceNotFound, parent)
	}
	entries, err := configured.client.propfind(ctx, parent, "1")
	if err != nil {
		return nil, fmt.Errorf("svn list %s: %w", parent, err)
	}
	out := make([]types.Resource, 0, len(entries))
	for _, entry := range entries {
		if entry.RelPath == "" || !entry.Collection {
			continue
		}
		out = append(out, types.Resource{
			ExternalID:  entry.Href,
			Name:        path.Base(entry.RelPath),
			Type:        "directory",
			ParentID:    parent,
			URL:         entry.Href,
			ModifiedAt:  entry.Modified,
			HasChildren: true,
		})
	}
	return out, nil
}

// ResolveResourceAncestors has nothing to reveal: every directory is addressed
// by its own absolute URL, which the picker already stores.
func (c *Connector) ResolveResourceAncestors(
	context.Context, *types.DataSourceConfig, []string,
) ([]string, error) {
	return []string{}, nil
}

// FetchStream is the production sync path: it walks the selected subtrees,
// downloads only files whose revision moved since the last run, reports files
// that disappeared as deletions, and checkpoints after each repository so a
// timeout resumes instead of restarting.
func (c *Connector) FetchStream(
	ctx context.Context, ds *types.DataSourceConfig,
	old *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return c.sync(ctx, ds, old, h, false)
}

// FetchFullStream re-downloads every supported file while keeping the previous
// snapshot as the deletion-reconciliation baseline.
func (c *Connector) FetchFullStream(
	ctx context.Context, ds *types.DataSourceConfig,
	old *types.SyncCursor, h datasource.StreamHandler,
) (*types.SyncCursor, error) {
	return c.sync(ctx, ds, old, h, true)
}

// FetchAll is the compatibility full sync used by the batch path. It has no
// cursor, so it cannot report deletions; FetchAllFromCursor covers that case.
func (c *Connector) FetchAll(
	ctx context.Context, ds *types.DataSourceConfig, _ []string,
) ([]types.FetchedItem, error) {
	h := &collectHandler{}
	if _, err := c.sync(ctx, ds, nil, h, true); err != nil {
		return nil, err
	}
	return h.items, nil
}

// FetchIncremental is the compatibility incremental sync used by the batch path.
func (c *Connector) FetchIncremental(
	ctx context.Context, ds *types.DataSourceConfig, old *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	h := &collectHandler{}
	next, err := c.sync(ctx, ds, old, h, false)
	if err != nil {
		return nil, nil, err
	}
	return h.items, next, nil
}

// FetchAllFromCursor re-fetches everything while reconciling deletions against
// the previous snapshot (sync_mode=full on the batch path).
func (c *Connector) FetchAllFromCursor(
	ctx context.Context, ds *types.DataSourceConfig,
	_ []string, old *types.SyncCursor,
) ([]types.FetchedItem, *types.SyncCursor, error) {
	h := &collectHandler{}
	next, err := c.sync(ctx, ds, old, h, true)
	if err != nil {
		return nil, nil, err
	}
	return h.items, next, nil
}

// collectHandler implements StreamHandler by buffering, which lets the batch
// methods reuse the streaming implementation and inherit its exact semantics.
type collectHandler struct {
	items []types.FetchedItem
}

func (h *collectHandler) Emit(_ context.Context, item types.FetchedItem) error {
	h.items = append(h.items, item)
	return nil
}

func (h *collectHandler) Checkpoint(context.Context, *types.SyncCursor) error { return nil }

// sync walks every configured repository once and drives h. force emits every
// supported file (full sync); otherwise only files whose revision moved.
//
// The walk checkpoints after every configured path selection, not only after a
// whole repository. A single repository can hold dozens of large
// subtrees whose combined download+ingest exceeds one Asynq task timeout, and
// with a per-repository checkpoint every retry re-walked those subtrees from
// scratch and never finished — the "sync stuck running" symptom. Finer
// checkpoints let a timeout resume at the next unfinished selection. The
// baseline snapshot in Repositories is left untouched until the repository
// completes, so the deletion reconciliation keeps comparing against the last
// committed state; the partial work lives in Pending.
func (c *Connector) sync(
	ctx context.Context, ds *types.DataSourceConfig,
	old *types.SyncCursor, h datasource.StreamHandler, force bool,
) (*types.SyncCursor, error) {
	configured, cfg, err := c.configured(ds)
	if err != nil {
		return nil, err
	}
	previous := decodeCursor(old)
	next := cursorState{
		Repositories: make(map[string]repoSnapshot, len(cfg.Repositories)),
		Pending:      make(map[string]repoProgress),
	}

	for _, repo := range cfg.Repositories {
		// baseline is the last committed snapshot for this repository; it stays
		// the deletion baseline for the whole run even while checkpoints land.
		baseline := previous.Repositories[repo.URL]
		if baseline == nil {
			baseline = repoSnapshot{}
		}
		progress, resuming := previous.Pending[repo.URL]
		if !resuming || progress.Working == nil {
			progress.Working = make(repoSnapshot)
		}
		working := progress.Working
		done := make(map[string]struct{}, len(progress.Done))
		for _, selection := range progress.Done {
			done[selection] = struct{}{}
		}
		skips := &walkSkips{entries: skipsFromCursor(progress.Skips)}

		selections := repo.Paths
		if len(selections) == 0 {
			selections = []string{""}
		}

		visit := func(idPath string, entry davEntry) error {
			current := snapshotEntry{Rev: entry.Rev, Size: entry.Size, ETag: entry.ETag, MD5: entry.MD5}
			// Record a known-oversized file as skipped before the change test, so
			// an unchanged skip is not reported again on every run.
			if limit := configured.skipLimitFor(entry); limit > 0 {
				current.SkipLimit = limit
			}
			if !force {
				if prior, seen := baseline[idPath]; seen && sameRevision(prior, current) {
					working[idPath] = current
					return nil
				}
			}
			item, skipLimit, err := configured.item(ctx, repo, idPath, entry)
			if err != nil {
				// Credentials and a cancelled/expired context are fatal: every
				// further request would fail the same way. Any other per-file
				// failure (a connection reset, EOF, an upstream 5xx) is isolated
				// to this file — one undeliverable blob must not fail the whole
				// repository sync and strand every sibling document.
				if ctx.Err() != nil || errors.Is(err, datasource.ErrInvalidCredentials) {
					return err
				}
				logger.Warnf(ctx, "[SVN] file download failed, skipping it this run: %s: %v", idPath, err)
				// Carry a previously synced file forward so this transient
				// failure is not misread by the deletion pass as "removed from
				// the repository"; a brand-new file stays out of the snapshot so
				// the next run retries it.
				if prior, seen := baseline[idPath]; seen {
					working[idPath] = prior
				}
				return h.Emit(ctx, fetchFailedItem(repo, idPath, entry, err))
			}
			if skipLimit > 0 {
				current.SkipLimit = skipLimit
			}
			working[idPath] = current
			return h.Emit(ctx, item)
		}

		for _, selection := range selections {
			if _, finished := done[selection]; finished {
				continue
			}
			reported := len(skips.entries)
			walkErr := configured.walkSelection(ctx, repo, selection, visit, skips)
			if walkErr != nil {
				var gone *errPathGone
				if !errors.As(walkErr, &gone) {
					// Fatal (cancelled context, credentials, 5xx): keep the
					// selections already checkpointed so the retry resumes there.
					return nil, walkErr
				}
				// A configured path that does not exist is configuration drift,
				// not a failed sync: report it and move on.
				logger.Warnf(ctx, "[SVN] configured path does not exist, skipping it: %s", gone.url)
			}
			// A configured path the server does not have is reported once per run
			// as a failed item, so the operator sees which entry of the
			// repository list is stale without reading the server log.
			for _, skip := range skips.entries[reported:] {
				if err := h.Emit(ctx, missingPathItem(repo, skip)); err != nil {
					return nil, err
				}
			}
			progress.Done = append(progress.Done, selection)
			progress.Skips = skipsToCursor(skips.entries)
			done[selection] = struct{}{}
			next.Repositories[repo.URL] = baseline
			next.Pending[repo.URL] = progress
			if err := h.Checkpoint(ctx, encodeCursor(next)); err != nil {
				return nil, err
			}
		}

		// All selections are walked: anything the baseline tracked and the walk
		// did not see is gone from the repository (or left the selected paths)
		// and must be removed from the knowledge base. A subtree that could not
		// be read this run is the exception: its absence is unknown rather than
		// a deletion, so its entries are carried forward untouched. Deleting
		// them would let one mistyped path empty a whole directory.
		for idPath, entry := range baseline {
			if _, present := working[idPath]; present {
				continue
			}
			if skips.covers(idPath) {
				working[idPath] = entry
				continue
			}
			if err := h.Emit(ctx, deletedItem(repo.URL, idPath)); err != nil {
				return nil, err
			}
		}

		next.Repositories[repo.URL] = working
		delete(next.Pending, repo.URL)
		// The checkpoint must be a complete resumable snapshot: repositories not
		// yet visited are simply absent, so a resume re-walks them from scratch.
		if err := h.Checkpoint(ctx, encodeCursor(next)); err != nil {
			return nil, err
		}
	}
	return encodeCursor(next), nil
}

// errPathGone marks a directory the server reports as absent. It travels up only
// as far as the selection boundary that turns it into a skip; a directory
// discovered mid-walk is swallowed by its parent so one vanished subdirectory
// does not abandon its siblings.
type errPathGone struct {
	url string
}

func (e *errPathGone) Error() string {
	return fmt.Sprintf("svn path does not exist: %s", e.url)
}

// walkSkip is one directory a walk could not read.
type walkSkip struct {
	// idPath is the repository-relative directory, in the same vocabulary as
	// the cursor's snapshot keys.
	idPath string
	// url is the absolute URL that answered 404, reported to the operator.
	url string
}

// walkSkips collects the directories a walk could not read.
//
// A directory that answered 404 is unknown, not empty. Without this record the
// deletion pass in sync would read "not listed this run" as "deleted from the
// repository" and remove every document of a subtree that a mistyped or
// transiently missing path merely hid.
type walkSkips struct {
	entries []walkSkip
}

func (s *walkSkips) add(idPath, url string) {
	s.entries = append(s.entries, walkSkip{idPath: idPath, url: url})
}

// covers reports whether idPath sits inside a directory that could not be read.
func (s *walkSkips) covers(idPath string) bool {
	for _, skip := range s.entries {
		// An empty idPath is the repository root: the whole tree was unreadable,
		// so every tracked path is inside an unread subtree.
		if skip.idPath == "" || idPath == skip.idPath || strings.HasPrefix(idPath, skip.idPath+"/") {
			return true
		}
	}
	return false
}

// walkSelection walks one configured path subtree with its own entry budget. A
// configured path that the server does not have surfaces as *errPathGone so the
// caller can record it as a skip and keep the run going. The budget is per
// selection (not per repository) because each selection is an independently
// resumable checkpoint unit.
func (c *Connector) walkSelection(
	ctx context.Context, repo repository, selection string,
	visit func(idPath string, entry davEntry) error, skips *walkSkips,
) error {
	budget := maxWalkEntries
	return c.walkDir(ctx, directoryURL(repo.URL, selection), selection, 0, &budget, visit, skips)
}

// walkDir lists one directory and recurses into its subdirectories. A Depth: 1
// walk per level (rather than one Depth: infinity PROPFIND) keeps memory bounded
// by the current listing instead of the whole repository tree.
func (c *Connector) walkDir(
	ctx context.Context, dirURL, base string, depth int, budget *int,
	visit func(idPath string, entry davEntry) error, skips *walkSkips,
) error {
	if depth > maxWalkDepth {
		return fmt.Errorf("svn %s: directory nesting exceeds %d levels", dirURL, maxWalkDepth)
	}
	entries, err := c.client.propfind(ctx, dirURL, "1")
	if err != nil {
		if isNotImplemented(err) {
			return fmt.Errorf(
				"svn %s did not accept WebDAV PROPFIND (%w); "+
					"the URL must be served by mod_dav_svn over http(s)",
				dirURL, err,
			)
		}
		if isPathGone(err) {
			// Record the unreadable subtree, then let the caller decide: a
			// configured selection becomes a skip, while a directory found
			// mid-walk is simply left behind.
			skips.add(base, dirURL)
			return &errPathGone{url: dirURL}
		}
		return fmt.Errorf("svn list %s: %w", dirURL, err)
	}
	for _, entry := range entries {
		if entry.RelPath == "" {
			continue // the requested collection itself
		}
		*budget--
		if *budget < 0 {
			return fmt.Errorf(
				"svn %s: repository tree exceeds %d entries; narrow the selected paths",
				dirURL, maxWalkEntries,
			)
		}
		idPath := path.Join(base, entry.RelPath)
		if entry.Collection {
			err := c.walkDir(ctx, entry.Href, idPath, depth+1, budget, visit, skips)
			var gone *errPathGone
			if errors.As(err, &gone) {
				// Already recorded by the recursive call; keep walking siblings.
				continue
			}
			if err != nil {
				return err
			}
			continue
		}
		if !isSupportedFile(entry.RelPath) {
			continue
		}
		if err := visit(idPath, entry); err != nil {
			return err
		}
	}
	return nil
}

// item downloads one repository file and maps it onto the knowledge model.
//
// The second result is the per-file limit that made the connector skip the
// file, or 0 when the file was ingested. The caller stores that limit in the
// snapshot, which keeps the decision idempotent (a skipped file is reported once
// instead of on every run) while still re-evaluating the file as soon as the
// operator raises MAX_FILE_SIZE_MB.
func (c *Connector) item(
	ctx context.Context, repo repository, idPath string, entry davEntry,
) (types.FetchedItem, int64, error) {
	// Reject known-oversized files before the GET: a repository may hold a
	// multi-gigabyte blob, and downloading it only to discard it would cost the
	// whole transfer every sync.
	if limit := c.skipLimitFor(entry); limit > 0 {
		return oversizedItem(repo, idPath, entry, limit), limit, nil
	}
	body, err := c.client.get(ctx, entry.Href, entry.Size)
	if err != nil {
		var tooLarge errFileTooLarge
		if errors.As(err, &tooLarge) {
			return oversizedItem(repo, idPath, entry, tooLarge.limit), tooLarge.limit, nil
		}
		return types.FetchedItem{}, 0, fmt.Errorf("svn file %s: %w", idPath, err)
	}
	name := repositoryName(repo.URL)
	return types.FetchedItem{
		ExternalID:  svnExternalID(repo.URL, idPath),
		Title:       name + "/" + idPath,
		FileName:    path.Join(name, idPath),
		Content:     body,
		ContentType: "text/plain",
		URL:         entry.Href,
		UpdatedAt:   entry.Modified,
		Metadata: map[string]string{
			"channel":        types.ConnectorTypeSVN,
			"source_type":    "svn",
			"svn_repository": repo.URL,
			"svn_path":       idPath,
			"svn_revision":   entry.Rev,
			"svn_url":        entry.Href,
			"svn_repo_path":  entry.RepoPath,
		},
	}, 0, nil
}

// skipLimitFor reports the per-file limit that excludes this entry from the
// sync, or 0 when the file is small enough to fetch. A listing without
// getcontentlength returns 0: the size is only known once the body arrives.
func (c *Connector) skipLimitFor(entry davEntry) int64 {
	if entry.Size > c.client.maxFileBytes {
		return c.client.maxFileBytes
	}
	return 0
}

// oversizedItem reports one skipped file as a failed sync item. It must carry no
// Content and no URL: the engine classifies an item as a connector failure only
// when both are empty, and a URL would make it fetch the very file the size
// limit exists to keep out of the knowledge base.
func oversizedItem(repo repository, idPath string, entry davEntry, limit int64) types.FetchedItem {
	reason := fmt.Sprintf(
		"file is larger than the %s per-file limit; "+
			"raise MAX_FILE_SIZE_MB (app and docreader) or lower the file's size in the repository",
		humanBytes(limit),
	)
	if entry.Size >= 0 {
		reason = fmt.Sprintf("file is %s; %s", humanBytes(entry.Size), reason)
	}
	return types.FetchedItem{
		ExternalID: svnExternalID(repo.URL, idPath),
		Title:      repositoryName(repo.URL) + "/" + idPath,
		FileName:   path.Join(repositoryName(repo.URL), idPath),
		UpdatedAt:  entry.Modified,
		Metadata: map[string]string{
			"channel":                 types.ConnectorTypeSVN,
			"source_type":             "svn",
			"svn_repository":          repo.URL,
			"svn_path":                idPath,
			"svn_revision":            entry.Rev,
			"svn_url":                 entry.Href,
			"error":                   reason,
			"error_reason_code":       "file_too_large",
			"error_reason_code_value": humanBytes(limit),
			"error_reason":            reason,
		},
	}
}

// fetchFailedItem reports one file the server listed but could not deliver (a
// dropped connection, EOF, or a transient upstream error). Like oversizedItem it
// carries no Content and no URL — the engine classifies an item as a connector
// failure only when both are empty, and a URL would make it re-fetch the very
// file that just failed — and it keeps the document the connector already had
// from being deleted, because the snapshot entry is carried forward by the
// caller. The file is retried on the next sync.
func fetchFailedItem(repo repository, idPath string, entry davEntry, cause error) types.FetchedItem {
	reason := fmt.Sprintf(
		"SVN file %q could not be downloaded this run and will be retried on the next sync: %v",
		idPath, cause,
	)
	return types.FetchedItem{
		ExternalID: svnExternalID(repo.URL, idPath),
		Title:      repositoryName(repo.URL) + "/" + idPath,
		UpdatedAt:  entry.Modified,
		Metadata: map[string]string{
			"channel":                 types.ConnectorTypeSVN,
			"source_type":             "svn",
			"svn_repository":          repo.URL,
			"svn_path":                idPath,
			"svn_revision":            entry.Rev,
			"svn_url":                 entry.Href,
			"error":                   reason,
			"error_reason_code":       "svn_fetch_failed",
			"error_reason_code_value": idPath,
			"error_reason":            reason,
		},
	}
}

// missingPathItem reports one configured directory the server does not have.
// Like oversizedItem it carries no Content and no URL — the engine classifies an
// item as a connector failure only when both are empty, and a URL would make it
// try to fetch the very directory that is missing — and it exists so the stale
// entry shows up in the sync log, not only in the server log.
func missingPathItem(repo repository, skip walkSkip) types.FetchedItem {
	name := repositoryName(repo.URL)
	title := name
	if skip.idPath != "" {
		title = name + "/" + skip.idPath
	}
	reason := fmt.Sprintf(
		"SVN path %q does not exist under %s, so it was skipped. "+
			"Correct the repository path list of this data source",
		skip.idPath, repo.URL,
	)
	return types.FetchedItem{
		ExternalID: svnExternalID(repo.URL, skip.idPath),
		Title:      title,
		Metadata: map[string]string{
			"channel":                 types.ConnectorTypeSVN,
			"source_type":             "svn",
			"svn_repository":          repo.URL,
			"svn_path":                skip.idPath,
			"svn_url":                 skip.url,
			"error":                   reason,
			"error_reason_code":       "svn_path_not_found",
			"error_reason_code_value": skip.idPath,
			"error_reason":            reason,
		},
	}
}

func deletedItem(repoURL, idPath string) types.FetchedItem {
	return types.FetchedItem{
		ExternalID: svnExternalID(repoURL, idPath),
		IsDeleted:  true,
		Metadata: map[string]string{
			"channel":        types.ConnectorTypeSVN,
			"svn_repository": repoURL,
			"svn_path":       idPath,
		},
	}
}

func belongsToConfiguredRepository(cfg *config, target string) bool {
	for _, repo := range cfg.Repositories {
		if target == repo.URL || strings.HasPrefix(target, repo.URL+"/") {
			return true
		}
	}
	return false
}

// snapshotEntry is one tracked file. Rev is the primary change signal; MD5 and
// ETag back it up on servers that do not expose DAV:version-name.
type snapshotEntry struct {
	Rev  string `json:"rev,omitempty"`
	Size int64  `json:"size,omitempty"`
	ETag string `json:"etag,omitempty"`
	MD5  string `json:"md5,omitempty"`
	// SkipLimit records that the file was left out of the knowledge base
	// because it is larger than this per-file limit. A non-zero value means "not
	// ingested, and here is why", so a later run with a raised limit sees the
	// difference and fetches the file.
	SkipLimit int64 `json:"skip_limit,omitempty"`
}

type repoSnapshot map[string]snapshotEntry

// repoProgress is the resumable progress of a repository a previous run stopped
// inside of. It is persisted only while a repository is unfinished;
// a completed repository commits to cursorState.Repositories and drops it.
type repoProgress struct {
	// Working holds the entries seen so far for the selections that already
	// completed. Entries for selections not yet walked are deliberately absent:
	// they are carried by the untouched baseline in Repositories, which stays
	// the deletion baseline for the whole repository.
	Working repoSnapshot `json:"working"`
	// Done lists the configured selections fully walked in earlier attempts.
	Done []string `json:"done"`
	// Skips lists the subtrees that could not be read, so a resumed run still
	// protects their tracked files from being misread as deletions.
	Skips []cursorSkip `json:"skips,omitempty"`
}

// cursorSkip is the cursor-safe form of walkSkip (whose fields are unexported).
type cursorSkip struct {
	IDPath string `json:"id_path"`
	URL    string `json:"url"`
}

type cursorState struct {
	Repositories map[string]repoSnapshot `json:"repositories"`
	// Pending holds the partial progress of repositories a previous run stopped
	// inside of, keyed by repository URL. Absent for completed repositories.
	Pending map[string]repoProgress `json:"pending,omitempty"`
}

func skipsToCursor(entries []walkSkip) []cursorSkip {
	if len(entries) == 0 {
		return nil
	}
	out := make([]cursorSkip, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cursorSkip{IDPath: entry.idPath, URL: entry.url})
	}
	return out
}

func skipsFromCursor(entries []cursorSkip) []walkSkip {
	if len(entries) == 0 {
		return nil
	}
	out := make([]walkSkip, 0, len(entries))
	for _, entry := range entries {
		out = append(out, walkSkip{idPath: entry.IDPath, url: entry.URL})
	}
	return out
}

// sameRevision reports whether a tracked file is in the same state as the last
// run: unchanged content, and the same ingest decision. SVN's last-changed
// revision is authoritative when both sides have one: the revision changes if
// and only if the file's content or properties changed. The skip limit is
// compared first, because a file that was skipped for size must be picked up
// again when the operator raises the limit, even if it never changed.
func sameRevision(prior, current snapshotEntry) bool {
	if prior.SkipLimit != current.SkipLimit {
		return false
	}
	switch {
	case prior.Rev != "" && current.Rev != "":
		return prior.Rev == current.Rev
	case prior.MD5 != "" && current.MD5 != "":
		return prior.MD5 == current.MD5
	default:
		return prior.ETag == current.ETag && prior.Size == current.Size
	}
}

// encodeCursor stores the snapshot as a JSON string under a single key. Keeping
// it opaque means the cursor survives a JSON round trip through the data_sources
// jsonb column without depending on how the service re-types nested maps.
func encodeCursor(state cursorState) *types.SyncCursor {
	if state.Repositories == nil {
		state.Repositories = map[string]repoSnapshot{}
	}
	if len(state.Pending) == 0 {
		state.Pending = nil
	}
	raw, err := json.Marshal(state)
	if err != nil {
		raw = []byte(`{"repositories":{}}`)
	}
	return &types.SyncCursor{
		LastSyncTime:    time.Now().UTC(),
		ConnectorCursor: map[string]interface{}{cursorStateKey: string(raw)},
	}
}

const cursorStateKey = "state"

func decodeCursor(cursor *types.SyncCursor) cursorState {
	state := cursorState{Repositories: map[string]repoSnapshot{}, Pending: map[string]repoProgress{}}
	if cursor == nil || cursor.ConnectorCursor == nil {
		return state
	}
	raw, ok := cursor.ConnectorCursor[cursorStateKey].(string)
	if !ok || raw == "" {
		return state
	}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return cursorState{Repositories: map[string]repoSnapshot{}, Pending: map[string]repoProgress{}}
	}
	if state.Repositories == nil {
		state.Repositories = map[string]repoSnapshot{}
	}
	if state.Pending == nil {
		state.Pending = map[string]repoProgress{}
	}
	return state
}

func isSupportedFile(file string) bool {
	_, ok := svnSupportedFileExtensions[strings.ToLower(path.Ext(file))]
	return ok
}

// svnSupportedFileExtensions limits repository sync to formats the knowledge
// import pipeline can process, matching the GitLab connector: a repository holds
// arbitrary blobs, unlike document-centric sources.
//
// Image and audio blobs are deliberately absent. WeKnora only imports those when
// the knowledge base has a VLM or ASR model configured, and otherwise rejects the
// file before storing it; syncing them here would download every picture and
// recording in the repository only to discard it, and would leave the path in the
// snapshot, so later runs skip it as "already synced" and never retry it once the
// model is configured. Add the extensions back together with the model.
var svnSupportedFileExtensions = map[string]struct{}{
	".pdf": {}, ".txt": {}, ".docx": {}, ".doc": {}, ".epub": {},
	".html": {}, ".htm": {}, ".mhtml": {}, ".md": {}, ".markdown": {}, ".mdx": {},
	".csv": {}, ".xlsx": {}, ".xls": {}, ".pptx": {}, ".ppt": {}, ".json": {},
}
