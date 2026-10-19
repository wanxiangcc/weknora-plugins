package svn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
)

// fakeNode is one entry of the fake mod_dav_svn tree.
type fakeNode struct {
	dir     bool
	rev     int
	content string
	// reportedSize overrides the size PROPFIND advertises, so a test can model
	// a multi-hundred-megabyte blob without materialising it. The GET handler
	// still returns content, which is what the connector would see if it
	// downloaded the file anyway.
	reportedSize int64
	// omitSize drops getcontentlength from the listing entirely, which is the
	// one case where the connector cannot judge a file before downloading it.
	omitSize bool
}

// defaultTestMaxFileBytes keeps direct client construction in tests explicit
// about the per-file limit they exercise.
const defaultTestMaxFileBytes int64 = 512 << 20

// fakeRepo serves a minimal mod_dav_svn: Depth 0/1 PROPFIND multistatus,
// GET for file bodies, and optional basic auth. It exists so the connector's
// traversal, change detection and deletion reconciliation can be exercised
// without a Subversion server.
type fakeRepo struct {
	mu             sync.Mutex
	root           string // URI prefix, e.g. "/svn/repo"
	nodes          map[string]fakeNode
	requests       []string
	propfindStatus int
	requireAuth    string
	// gone marks paths that their parent still lists but that answer 404 when
	// listed themselves, modelling a directory removed between the parent
	// listing and the recursion into it.
	gone map[string]bool
	// dropGet holds, per file path, how many upcoming GETs should have their
	// connection dropped mid-response, modelling a mod_dav_svn that resets the
	// connection under load. A positive count that outlasts the client's retries
	// makes the download fail permanently for that path.
	dropGet map[string]int
}

func newFakeRepo(root string) *fakeRepo {
	return &fakeRepo{
		root:    root,
		nodes:   map[string]fakeNode{"": {dir: true, rev: 1}},
		gone:    map[string]bool{},
		dropGet: map[string]int{},
	}
}

// markGone makes PROPFIND answer 404 for a path while leaving it visible in its
// parent's listing.
func (f *fakeRepo) markGone(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gone[path] = true
}

// failGet makes the next `times` GETs for path drop their connection without a
// response, which the client observes as a transport error.
func (f *fakeRepo) failGet(path string, times int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropGet[path] = times
}

// clearGone restores a path marked by markGone.
func (f *fakeRepo) clearGone(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.gone, path)
}

func (f *fakeRepo) put(path string, node fakeNode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if node.dir {
		node.content = ""
	}
	f.nodes[path] = node
}

func (f *fakeRepo) remove(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, path)
}

func (f *fakeRepo) relative(uriPath string) string {
	trimmed := strings.TrimPrefix(uriPath, f.root)
	return strings.Trim(trimmed, "/")
}

func parentPath(path string) string {
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[:idx]
	}
	return ""
}

func escapePath(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/")
}

func (f *fakeRepo) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.Path+" depth="+r.Header.Get("Depth"))
	if f.requireAuth != "" && r.Header.Get("Authorization") != f.requireAuth {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch r.Method {
	case "PROPFIND":
		f.propfind(w, r)
	case http.MethodGet:
		f.get(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeRepo) propfind(w http.ResponseWriter, r *http.Request) {
	if f.propfindStatus != 0 {
		w.WriteHeader(f.propfindStatus)
		return
	}
	dir := f.relative(r.URL.Path)
	if f.gone[dir] {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if _, ok := f.nodes[dir]; !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	entries := []string{dir}
	if r.Header.Get("Depth") != "0" {
		for path := range f.nodes {
			if path != dir && parentPath(path) == dir {
				entries = append(entries, path)
			}
		}
	}
	sort.Strings(entries)

	var body strings.Builder
	body.WriteString(`<?xml version="1.0" encoding="utf-8"?>` + "\n")
	body.WriteString(`<D:multistatus xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/dav/">` + "\n")
	for _, entry := range entries {
		body.WriteString(f.renderEntry(entry))
	}
	body.WriteString("</D:multistatus>")

	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = w.Write([]byte(body.String()))
}

func (f *fakeRepo) renderEntry(dir string) string {
	node := f.nodes[dir]
	href := f.root
	if dir != "" {
		href += "/" + escapePath(dir)
	}
	if node.dir {
		href += "/"
	}
	var props strings.Builder
	props.WriteString("<D:resourcetype>")
	if node.dir {
		props.WriteString("<D:collection/>")
	}
	props.WriteString("</D:resourcetype>")
	fmt.Fprintf(&props, "<D:version-name>%d</D:version-name>", node.rev)
	props.WriteString("<D:creationdate>2024-01-02T03:04:05Z</D:creationdate>")
	props.WriteString("<D:getlastmodified>Tue, 02 Jan 2024 03:04:05 GMT</D:getlastmodified>")
	fmt.Fprintf(&props, "<S:baseline-relative-path>%s</S:baseline-relative-path>", dir)
	if !node.dir {
		switch {
		case node.omitSize:
			// no getcontentlength: the connector only learns the size on GET
		case node.reportedSize > 0:
			fmt.Fprintf(&props, "<D:getcontentlength>%d</D:getcontentlength>", node.reportedSize)
		default:
			fmt.Fprintf(&props, "<D:getcontentlength>%d</D:getcontentlength>", len(node.content))
		}
		fmt.Fprintf(&props, "<D:getetag>&quot;%s-%d&quot;</D:getetag>", escapePath(dir), node.rev)
		fmt.Fprintf(&props, "<S:md5-checksum>%d</S:md5-checksum>", node.rev)
	}
	return "<D:response><D:href>" + href + "</D:href><D:propstat><D:prop>" +
		props.String() + "</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>\n"
}

func (f *fakeRepo) get(w http.ResponseWriter, r *http.Request) {
	dir := f.relative(r.URL.Path)
	if f.dropGet[dir] > 0 {
		f.dropGet[dir]--
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
			return
		}
	}
	node, ok := f.nodes[dir]
	if !ok || node.dir {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	_, _ = w.Write([]byte(node.content))
}

// buildRepo returns a repository with a couple of directories, a mix of
// supported and unsupported file types.
func buildRepo() *fakeRepo {
	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 2})
	repo.put("trunk/docs", fakeNode{dir: true, rev: 3})
	repo.put("trunk/docs/guide.md", fakeNode{rev: 4, content: "# guide v1"})
	repo.put("trunk/notes.txt", fakeNode{rev: 5, content: "notes"})
	repo.put("trunk/logo.docx", fakeNode{rev: 6, content: "docx-bytes"})
	repo.put("trunk/build.bin", fakeNode{rev: 7, content: "unsupported"})
	return repo
}

func svnConfig(t *testing.T, baseURL string, repositories []interface{}) *types.DataSourceConfig {
	t.Helper()
	settings := map[string]interface{}{}
	if repositories != nil {
		settings["repositories"] = repositories
	}
	return &types.DataSourceConfig{
		Type:        types.ConnectorTypeSVN,
		Credentials: map[string]interface{}{"base_url": baseURL, "username": "alice", "password": "secret"},
		Settings:    settings,
	}
}

func newTestServer(t *testing.T, repo *fakeRepo) (*httptest.Server, string) {
	t.Helper()
	t.Setenv("SSRF_WHITELIST", "127.0.0.1,::1,localhost")
	server := httptest.NewServer(http.HandlerFunc(repo.serveHTTP))
	t.Cleanup(server.Close)
	return server, server.URL + "/svn/repo"
}

func itemPaths(items []types.FetchedItem) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Metadata["svn_path"])
	}
	sort.Strings(out)
	return out
}

// requestLog snapshots the requests the fake server has seen.
func (f *fakeRepo) requestLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.requests...)
}

// itemsByPath indexes emitted items by their repository path.
func itemsByPath(items []types.FetchedItem) map[string]types.FetchedItem {
	out := make(map[string]types.FetchedItem, len(items))
	for _, item := range items {
		out[item.Metadata["svn_path"]] = item
	}
	return out
}

func TestParseConfigValidatesURLs(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")

	t.Run("missing base url", func(t *testing.T) {
		_, err := parseConfig(&types.DataSourceConfig{Type: types.ConnectorTypeSVN})
		if err == nil || !strings.Contains(err.Error(), "SVN url is required") {
			t.Fatalf("want missing-url error, got %v", err)
		}
	})

	t.Run("svn scheme rejected", func(t *testing.T) {
		_, err := parseConfig(&types.DataSourceConfig{
			Type:        types.ConnectorTypeSVN,
			Credentials: map[string]interface{}{"base_url": "svn://svn.example.com/repo"},
		})
		if err == nil || !strings.Contains(err.Error(), "must use http(s)") {
			t.Fatalf("want scheme error, got %v", err)
		}
	})

	t.Run("credentials must not be embedded", func(t *testing.T) {
		_, err := parseConfig(&types.DataSourceConfig{
			Type:        types.ConnectorTypeSVN,
			Credentials: map[string]interface{}{"base_url": "https://alice:pw@svn.example.com/repo"},
		})
		if err == nil || !strings.Contains(err.Error(), "must not embed credentials") {
			t.Fatalf("want embedded-credential error, got %v", err)
		}
	})

	t.Run("password without username", func(t *testing.T) {
		_, err := parseConfig(&types.DataSourceConfig{
			Type: types.ConnectorTypeSVN,
			Credentials: map[string]interface{}{
				"base_url": "http://127.0.0.1/repo", "password": "pw",
			},
		})
		if err == nil || !strings.Contains(err.Error(), "password requires a username") {
			t.Fatalf("want username error, got %v", err)
		}
	})

	t.Run("empty repository list falls back to base url", func(t *testing.T) {
		cfg, err := parseConfig(svnConfig(t, "http://127.0.0.1/repo/", nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Repositories) != 1 || cfg.Repositories[0].URL != "http://127.0.0.1/repo" {
			t.Fatalf("want base_url as the only repository, got %+v", cfg.Repositories)
		}
	})

	t.Run("duplicate urls merge paths", func(t *testing.T) {
		cfg, err := parseConfig(svnConfig(t, "http://127.0.0.1/repo", []interface{}{
			map[string]interface{}{"url": "http://127.0.0.1/repo", "paths": []interface{}{"trunk/docs"}},
			map[string]interface{}{"url": "http://127.0.0.1/repo/", "paths": []interface{}{"trunk/src"}},
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(cfg.Repositories) != 1 {
			t.Fatalf("want one merged repository, got %+v", cfg.Repositories)
		}
		paths := cfg.Repositories[0].Paths
		if len(paths) != 2 || paths[0] != "trunk/docs" || paths[1] != "trunk/src" {
			t.Fatalf("want merged paths, got %v", paths)
		}
	})
}

func TestValidateProbesBaseURL(t *testing.T) {
	repo := newFakeRepo("/svn/repo")
	repo.requireAuth = "Basic " + basicAuth("alice", "secret")
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	if err := connector.Validate(context.Background(), svnConfig(t, baseURL, nil)); err != nil {
		t.Fatalf("validate with good credentials: %v", err)
	}

	bad := svnConfig(t, baseURL, nil)
	bad.Credentials["password"] = "wrong"
	err := connector.Validate(context.Background(), bad)
	if err == nil || !strings.Contains(err.Error(), "invalid credentials") {
		t.Fatalf("want credentials error, got %v", err)
	}
}

func basicAuth(user, password string) string {
	req, _ := http.NewRequest(http.MethodGet, "http://example.invalid", nil)
	req.SetBasicAuth(user, password)
	return strings.TrimPrefix(req.Header.Get("Authorization"), "Basic ")
}

func TestListResourcesWalksOneLevelAtATime(t *testing.T) {
	repo := buildRepo()
	repo.requireAuth = "Basic " + basicAuth("alice", "secret")
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	root, err := connector.ListResources(context.Background(), ds, "")
	if err != nil {
		t.Fatalf("list root: %v", err)
	}
	if len(root) != 1 || root[0].Type != "repository" || root[0].ExternalID != baseURL {
		t.Fatalf("unexpected root resources: %+v", root)
	}
	if !root[0].HasChildren {
		t.Fatalf("repository should be expandable: %+v", root[0])
	}

	children, err := connector.ListResources(context.Background(), ds, root[0].ExternalID)
	if err != nil {
		t.Fatalf("list trunk: %v", err)
	}
	if len(children) != 1 || children[0].Name != "trunk" || children[0].Type != "directory" {
		t.Fatalf("unexpected trunk listing: %+v", children)
	}

	nested, err := connector.ListResources(context.Background(), ds, children[0].ExternalID)
	if err != nil {
		t.Fatalf("list docs: %v", err)
	}
	names := []string{}
	for _, resource := range nested {
		names = append(names, resource.Name)
	}
	if len(names) != 1 || names[0] != "docs" {
		t.Fatalf("want only directories below trunk, got %v", names)
	}

	// The picker must not be usable as an SSRF probe for arbitrary hosts.
	if _, err := connector.ListResources(context.Background(), ds, "http://169.254.169.254/latest/meta-data"); err == nil {
		t.Fatal("want error for a URL outside the configured repositories")
	}
}

func TestFetchStreamFullIncrementalAndDeletions(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	// Full run: every supported file, no binary blob.
	full := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, full)
	if err != nil {
		t.Fatalf("full stream: %v", err)
	}
	if got := itemPaths(full.items); strings.Join(got, ",") != "trunk/docs/guide.md,trunk/logo.docx,trunk/notes.txt" {
		t.Fatalf("unexpected full items: %v", got)
	}
	for _, item := range full.items {
		if item.IsDeleted {
			t.Fatalf("full sync must not report deletions: %+v", item)
		}
		if item.Metadata["channel"] != types.ConnectorTypeSVN {
			t.Fatalf("missing channel metadata: %+v", item.Metadata)
		}
		if item.URL == "" || item.Metadata["svn_url"] != item.URL {
			t.Fatalf("item must carry its repository URL: %+v", item)
		}
		if item.UpdatedAt.IsZero() {
			t.Fatalf("creationdate should populate UpdatedAt: %+v", item)
		}
	}

	// The cursor must survive a JSON round trip through the data_sources jsonb
	// column, which retypes nested maps as map[string]interface{}.
	raw, err := json.Marshal(cursor)
	if err != nil {
		t.Fatalf("marshal cursor: %v", err)
	}
	var restored types.SyncCursor
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatalf("unmarshal cursor: %v", err)
	}

	// Nothing changed: no items, no deletions.
	unchanged := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, &restored, unchanged); err != nil {
		t.Fatalf("no-op stream: %v", err)
	}
	if len(unchanged.items) != 0 {
		t.Fatalf("unchanged repository must emit nothing, got %v", itemPaths(unchanged.items))
	}

	// One file changed: only that file is re-downloaded.
	repo.put("trunk/docs/guide.md", fakeNode{rev: 8, content: "# guide v2"})
	changed := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, &restored, changed); err != nil {
		t.Fatalf("incremental stream: %v", err)
	}
	if len(changed.items) != 1 || changed.items[0].Metadata["svn_path"] != "trunk/docs/guide.md" {
		t.Fatalf("want exactly the changed file, got %v", itemPaths(changed.items))
	}
	if string(changed.items[0].Content) != "# guide v2" {
		t.Fatalf("changed file must carry its new content, got %q", changed.items[0].Content)
	}

	// One file deleted: reported as a deletion, not silently dropped.
	deleted := &collectHandler{}
	repo.remove("trunk/notes.txt")
	if _, err := connector.FetchStream(context.Background(), ds, &restored, deleted); err != nil {
		t.Fatalf("delete stream: %v", err)
	}
	var deletions []types.FetchedItem
	for _, item := range deleted.items {
		if item.IsDeleted {
			deletions = append(deletions, item)
		}
	}
	if len(deletions) != 1 || deletions[0].Metadata["svn_path"] != "trunk/notes.txt" {
		t.Fatalf("want one deletion for notes.txt, got %+v", deleted.items)
	}
	if want := "svn:" + baseURL + ":trunk/notes.txt"; deletions[0].ExternalID != want {
		t.Fatalf("deletion external id = %q, want %q", deletions[0].ExternalID, want)
	}
}

func TestFetchFullStreamReconcilesDeletions(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}

	repo.remove("trunk/logo.docx")
	repo.put("trunk/notes.txt", fakeNode{rev: 9, content: "notes v2"})

	full := &collectHandler{}
	if _, err := connector.FetchFullStream(context.Background(), ds, cursor, full); err != nil {
		t.Fatalf("full stream: %v", err)
	}
	var deleted, updated int
	for _, item := range full.items {
		if item.IsDeleted {
			deleted++
			continue
		}
		updated++
	}
	if updated != 2 {
		t.Fatalf("full sync must re-emit every remaining file, got %d (%v)", updated, itemPaths(full.items))
	}
	if deleted != 1 {
		t.Fatalf("full sync must reconcile deletions, got %d", deleted)
	}
}

func TestFetchIncrementalHonoursPathSelection(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, []interface{}{
		map[string]interface{}{"url": baseURL, "paths": []interface{}{"trunk/docs"}},
	})

	items, cursor, err := connector.FetchIncremental(context.Background(), ds, nil)
	if err != nil {
		t.Fatalf("incremental fetch: %v", err)
	}
	if len(items) != 1 || items[0].Metadata["svn_path"] != "trunk/docs/guide.md" {
		t.Fatalf("path filter must limit the sync, got %v", itemPaths(items))
	}
	if cursor == nil {
		t.Fatal("incremental fetch must return a cursor")
	}
}

// A repository selection routinely holds dozens of paths. One that no longer
// exists in Subversion is configuration drift, and must be skipped and reported
// rather than aborting the sync of every other directory.
func TestMissingConfiguredPathIsSkippedNotFatal(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, []interface{}{
		map[string]interface{}{"url": baseURL, "paths": []interface{}{"trunk/docs", "trunk/removed"}},
	})

	handler := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, handler)
	if err != nil {
		t.Fatalf("a missing configured path must not fail the sync: %v", err)
	}
	if cursor == nil {
		t.Fatal("a sync that skipped a path must still return a cursor")
	}

	byPath := itemsByPath(handler.items)
	if _, ok := byPath["trunk/docs/guide.md"]; !ok {
		t.Fatalf("the healthy path must still sync, got %v", itemPaths(handler.items))
	}
	skip, ok := byPath["trunk/removed"]
	if !ok {
		t.Fatalf("the missing path must be reported, got %v", itemPaths(handler.items))
	}
	if skip.IsDeleted {
		t.Fatalf("a missing configured path is not a deletion: %+v", skip)
	}
	// The engine classifies an item as a connector failure only when both are
	// empty; a URL would make it fetch the directory that does not exist.
	if skip.Content != nil || skip.URL != "" {
		t.Fatalf("a reported skip must carry no content and no URL: %+v", skip)
	}
	if got := skip.Metadata["error_reason_code"]; got != "svn_path_not_found" {
		t.Fatalf("error_reason_code = %q, want svn_path_not_found", got)
	}
	if skip.Metadata["error"] == "" || skip.Metadata["svn_url"] == "" {
		t.Fatalf("a reported skip must carry a reason and the URL that 404ed: %+v", skip.Metadata)
	}
}

// The dangerous half of "skip a path": its files are absent from the walk simply
// because the walk could not read them. Reporting them as deletions would let one
// unreadable path empty a directory of the knowledge base.
func TestUnreadablePathDoesNotDeletePreviouslySyncedFiles(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, []interface{}{
		map[string]interface{}{"url": baseURL, "paths": []interface{}{"trunk/docs"}},
	})

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}
	if _, ok := itemsByPath(first.items)["trunk/docs/guide.md"]; !ok {
		t.Fatalf("first sync must ingest the file, got %v", itemPaths(first.items))
	}

	// The path stays selected but the server no longer lists it (a rename, a
	// transient 404, or a typo introduced after the first sync). Its files are
	// unreachable, not deleted, so nothing may be removed from the knowledge base.
	repo.markGone("trunk/docs")
	second := &collectHandler{}
	next, err := connector.FetchStream(context.Background(), ds, cursor, second)
	if err != nil {
		t.Fatalf("second stream: %v", err)
	}
	for _, item := range second.items {
		if item.IsDeleted {
			t.Fatalf("an unreadable path must not delete already-synced documents: %+v", item)
		}
	}

	// The file must still be tracked, so recovery is a no-op rather than a
	// re-ingest (and never a re-create).
	repo.clearGone("trunk/docs")
	third := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, next, third); err != nil {
		t.Fatalf("third stream: %v", err)
	}
	if len(third.items) != 0 {
		t.Fatalf("a file preserved across a skip must stay in the snapshot, got %v", itemPaths(third.items))
	}
}

// Dropping a path from the repository list is a deliberate edit, not an outage:
// its files leave the selection and are still reconciled as deletions.
func TestRemovedPathStillReconcilesDeletions(t *testing.T) {
	repo := buildRepo()
	repo.put("trunk/other", fakeNode{dir: true, rev: 8})
	repo.put("trunk/other/plan.md", fakeNode{rev: 9, content: "# plan"})
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	both := svnConfig(t, baseURL, []interface{}{
		map[string]interface{}{"url": baseURL, "paths": []interface{}{"trunk/docs", "trunk/other"}},
	})
	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), both, nil, first)
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}
	if _, ok := itemsByPath(first.items)["trunk/docs/guide.md"]; !ok {
		t.Fatalf("first sync must ingest the file, got %v", itemPaths(first.items))
	}

	// trunk/docs is dropped from the list and is perfectly readable, so its file
	// genuinely left the selection. Preserving it would keep deleted documents in
	// the knowledge base forever, so the skip bookkeeping must not cover it.
	otherOnly := svnConfig(t, baseURL, []interface{}{
		map[string]interface{}{"url": baseURL, "paths": []interface{}{"trunk/other"}},
	})
	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), otherOnly, cursor, second); err != nil {
		t.Fatalf("second stream: %v", err)
	}
	deletion, ok := itemsByPath(second.items)["trunk/docs/guide.md"]
	if !ok || !deletion.IsDeleted {
		t.Fatalf("a path dropped from the selection must still reconcile its deletions, got %v", itemPaths(second.items))
	}
}

// The whole repository going unreadable is the widest form of the same hazard:
// every tracked path is inside the unread subtree.
func TestUnreadableRepositoryRootPreservesEveryTrackedFile(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("first stream: %v", err)
	}
	if len(first.items) == 0 {
		t.Fatal("first sync must ingest the repository")
	}

	repo.markGone("")

	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, cursor, second); err != nil {
		t.Fatalf("an unreadable repository root must not fail the sync: %v", err)
	}
	for _, item := range second.items {
		if item.IsDeleted {
			t.Fatalf("an unreadable root must not delete the knowledge base: %+v", item)
		}
	}
	if _, ok := itemsByPath(second.items)[""]; !ok {
		t.Fatalf("the skipped root must be reported, got %v", itemPaths(second.items))
	}
}

// A directory deleted between its parent's listing and the recursion into it must
// not abandon its siblings.
func TestSubdirectoryVanishingMidWalkKeepsSiblings(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)
	repo.markGone("trunk/docs")

	connector := NewConnector()
	handler := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), svnConfig(t, baseURL, nil), nil, handler); err != nil {
		t.Fatalf("a subdirectory that vanished mid-walk must not abort the walk: %v", err)
	}

	byPath := itemsByPath(handler.items)
	if _, ok := byPath["trunk/notes.txt"]; !ok {
		t.Fatalf("siblings of the vanished directory must still sync, got %v", itemPaths(handler.items))
	}
	if _, ok := byPath["trunk/docs"]; !ok {
		t.Fatalf("the vanished directory must be reported, got %v", itemPaths(handler.items))
	}
}

// A failed listing is not an absent path: a 5xx or a 403 must keep failing the
// sync, because treating it as "gone" would silently drop a subtree.
func TestServerErrorStillFailsTheSync(t *testing.T) {
	repo := buildRepo()
	repo.propfindStatus = http.StatusInternalServerError
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	handler := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), svnConfig(t, baseURL, nil), nil, handler); err == nil {
		t.Fatal("a 500 from PROPFIND must fail the sync, not be skipped")
	}
}

// A single file the server refuses to deliver must not fail the whole
// repository sync: the siblings still sync and the bad file is reported once.
func TestFileDownloadFailureIsIsolated(t *testing.T) {
	repo := buildRepo()
	repo.failGet("trunk/notes.txt", 100)
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	handler := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), svnConfig(t, baseURL, nil), nil, handler); err != nil {
		t.Fatalf("one undeliverable file must not fail the whole sync: %v", err)
	}

	byPath := itemsByPath(handler.items)
	if _, ok := byPath["trunk/docs/guide.md"]; !ok {
		t.Fatalf("siblings of the failed file must still sync, got %v", itemPaths(handler.items))
	}
	bad, ok := byPath["trunk/notes.txt"]
	if !ok {
		t.Fatalf("the failed file must be reported, got %v", itemPaths(handler.items))
	}
	if len(bad.Content) != 0 || bad.URL != "" {
		t.Fatalf("a fetch failure must carry no content and no url: %+v", bad)
	}
	if bad.Metadata["error_reason_code"] != "svn_fetch_failed" {
		t.Fatalf("want error_reason_code=svn_fetch_failed, got %q", bad.Metadata["error_reason_code"])
	}
}

// A dropped connection is retried before it is surfaced, so a momentary reset
// does not even become a per-file failure.
func TestTransientDownloadErrorIsRetried(t *testing.T) {
	repo := buildRepo()
	repo.failGet("trunk/notes.txt", 1)
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	handler := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), svnConfig(t, baseURL, nil), nil, handler); err != nil {
		t.Fatalf("a single dropped connection must be retried, not surfaced: %v", err)
	}

	note, ok := itemsByPath(handler.items)["trunk/notes.txt"]
	if !ok || string(note.Content) != "notes" {
		t.Fatalf("the retried file must be delivered intact, got %+v", note)
	}
}

// A previously synced file that is transiently unreadable must be carried
// forward, not read as a deletion that would remove its document.
func TestTransientFailureDoesNotDeleteSyncedFile(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)
	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("seed sync: %v", err)
	}

	// The file changed upstream (so the change test forces a download) but the
	// server keeps dropping the GET.
	repo.put("trunk/notes.txt", fakeNode{rev: 99, content: "notes v2"})
	repo.failGet("trunk/notes.txt", 100)

	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, cursor, second); err != nil {
		t.Fatalf("sync must not fail on a transient download error: %v", err)
	}
	for _, item := range second.items {
		if item.IsDeleted && item.Metadata["svn_path"] == "trunk/notes.txt" {
			t.Fatal("a transiently unreadable file must not be reported as deleted")
		}
	}
	if _, ok := itemsByPath(second.items)["trunk/notes.txt"]; !ok {
		t.Fatal("the failed file must still be reported as a failure item")
	}
}

func TestFetchAllBuffersEveryFile(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	items, err := connector.FetchAll(context.Background(), svnConfig(t, baseURL, nil), nil)
	if err != nil {
		t.Fatalf("fetch all: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("want 3 supported files, got %v", itemPaths(items))
	}
	if !strings.HasPrefix(items[0].ExternalID, "svn:"+baseURL+":") {
		t.Fatalf("external id must be repository scoped: %q", items[0].ExternalID)
	}
}

func TestFetchAllFromCursorReconcilesDeletions(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)
	_, cursor, err := connector.FetchIncremental(context.Background(), ds, nil)
	if err != nil {
		t.Fatalf("seed cursor: %v", err)
	}

	repo.remove("trunk/logo.docx")
	items, _, err := connector.FetchAllFromCursor(context.Background(), ds, nil, cursor)
	if err != nil {
		t.Fatalf("fetch all from cursor: %v", err)
	}
	var deletions int
	for _, item := range items {
		if item.IsDeleted {
			deletions++
		}
	}
	if deletions != 1 {
		t.Fatalf("want one deletion, got %d", deletions)
	}
}

func TestUnsupportedExtensionsAreNotDownloaded(t *testing.T) {
	repo := buildRepo()
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	if _, err := connector.FetchAll(context.Background(), svnConfig(t, baseURL, nil), nil); err != nil {
		t.Fatalf("fetch all: %v", err)
	}
	for _, request := range repo.requests {
		if strings.HasSuffix(request, "build.bin depth=") {
			t.Fatalf("binary blob must not be fetched: %s", request)
		}
	}
}

func TestRelativePathHandlesHrefShapes(t *testing.T) {
	base, err := url.Parse("https://svn.example.com/svn/repo/trunk/docs")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		href string
		want string
	}{
		{"/svn/repo/trunk/docs/", ""},
		{"/svn/repo/trunk/docs", ""},
		{"/svn/repo/trunk/docs/a%20b.md", "a b.md"},
		{"https://svn.example.com/svn/repo/trunk/docs/sub/file.md", "sub/file.md"},
		{"/svn/repo/trunk/docs/sub/", "sub"},
	}
	for _, tc := range cases {
		got, err := relativePath(base, tc.href)
		if err != nil {
			t.Fatalf("relativePath(%q): %v", tc.href, err)
		}
		if got != tc.want {
			t.Errorf("relativePath(%q) = %q, want %q", tc.href, got, tc.want)
		}
	}
	if _, err := relativePath(base, "https://other.example.com/svn/elsewhere/x.md"); err == nil {
		t.Fatal("hrefs outside the collection must be rejected")
	}
}

func TestWalkRejectsOversizedTrees(t *testing.T) {
	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 1})
	for i := 0; i < 8; i++ {
		repo.put(fmt.Sprintf("trunk/file%d.md", i), fakeNode{rev: i + 1, content: "x"})
	}
	_, baseURL := newTestServer(t, repo)

	configured := &Connector{client: newClient("", "", defaultTestMaxFileBytes)}
	budget := 3
	err := configured.walkDir(context.Background(), baseURL+"/trunk", "trunk", 0, &budget,
		func(string, davEntry) error { return nil }, &walkSkips{})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("want entry-budget error, got %v", err)
	}
}

func TestParseMultiStatusIgnoresErrorPropstat(t *testing.T) {
	body := []byte(`<?xml version="1.0" encoding="utf-8"?>
<D:multistatus xmlns:D="DAV:" xmlns:S="http://subversion.tigris.org/xmlns/dav/">
  <D:response>
    <D:href>/svn/repo/trunk/</D:href>
    <D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
    <D:propstat><D:prop><S:md5-checksum/></D:prop>
      <D:status>HTTP/1.1 404 Not Found</D:status></D:propstat>
  </D:response>
  <D:response>
    <D:href>/svn/repo/trunk/a.md</D:href>
    <D:propstat><D:prop><D:resourcetype/><D:version-name>7</D:version-name>
      <D:getcontentlength>3</D:getcontentlength></D:prop>
      <D:status>HTTP/1.1 200 OK</D:status></D:propstat>
  </D:response>
</D:multistatus>`)
	entries, err := parseMultiStatus(body, "https://svn.example.com/svn/repo/trunk")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}
	if !entries[0].Collection || entries[0].RelPath != "" {
		t.Fatalf("collection entry mis-parsed: %+v", entries[0])
	}
	if entries[1].Collection || entries[1].Rev != "7" || entries[1].Size != 3 || entries[1].RelPath != "a.md" {
		t.Fatalf("file entry mis-parsed: %+v", entries[1])
	}
}

func TestSameRevisionPrefersRevisionOverETag(t *testing.T) {
	prior := snapshotEntry{Rev: "5", Size: 10, ETag: "old"}
	current := snapshotEntry{Rev: "5", Size: 99, ETag: "new"}
	if !sameRevision(prior, current) {
		t.Fatal("equal revisions must count as unchanged")
	}
	current.Rev = "6"
	if sameRevision(prior, current) {
		t.Fatal("a bumped revision must count as changed")
	}
	// Servers without version-name fall back to etag + size.
	prior = snapshotEntry{ETag: "abc", Size: 3}
	current = snapshotEntry{ETag: "abc", Size: 3}
	if !sameRevision(prior, current) {
		t.Fatal("equal etag and size must count as unchanged")
	}
	current.Size = 4
	if sameRevision(prior, current) {
		t.Fatal("a larger body with the same etag must count as changed")
	}
	// A skip marker is part of the tracked state: raising the limit must make
	// the file look changed so the next run fetches it.
	prior = snapshotEntry{Rev: "5", SkipLimit: 10 << 20}
	current = snapshotEntry{Rev: "5", SkipLimit: 10 << 20}
	if !sameRevision(prior, current) {
		t.Fatal("the same skip decision must count as unchanged")
	}
	current.SkipLimit = 0
	if sameRevision(prior, current) {
		t.Fatal("a raised limit must re-open the file")
	}
}

func TestDecodeCursorToleratesForeignState(t *testing.T) {
	if got := decodeCursor(nil); len(got.Repositories) != 0 {
		t.Fatalf("nil cursor must decode to an empty state, got %+v", got)
	}
	foreign := &types.SyncCursor{ConnectorCursor: map[string]interface{}{"projects": map[string]interface{}{"1": "abc"}}}
	if got := decodeCursor(foreign); len(got.Repositories) != 0 {
		t.Fatalf("a GitLab-style cursor must decode to an empty state, got %+v", got)
	}
	broken := &types.SyncCursor{ConnectorCursor: map[string]interface{}{cursorStateKey: "not json"}}
	if got := decodeCursor(broken); len(got.Repositories) != 0 {
		t.Fatalf("malformed state must decode to an empty state, got %+v", got)
	}
}

func TestTypeAndMetadataRegistration(t *testing.T) {
	connector := NewConnector()
	if connector.Type() != types.ConnectorTypeSVN {
		t.Fatalf("type = %q", connector.Type())
	}
	if _, ok := datasource.ConnectorMetadataRegistry[types.ConnectorTypeSVN]; !ok {
		t.Fatal("svn must be present in the connector metadata registry")
	}
}

func TestMaxFileSizeSettingNeverExceedsTheDeploymentLimit(t *testing.T) {
	t.Setenv("SSRF_WHITELIST", "127.0.0.1")
	const mb = 1 << 20

	t.Run("defaults to the deployment ingest limit", func(t *testing.T) {
		t.Setenv("MAX_FILE_SIZE_MB", "50")
		cfg, err := parseConfig(svnConfig(t, "http://127.0.0.1/repo", nil))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if cfg.MaxFileBytes != 50*mb {
			t.Fatalf("want the deployment limit, got %d", cfg.MaxFileBytes)
		}
	})

	t.Run("a smaller setting narrows the limit", func(t *testing.T) {
		t.Setenv("MAX_FILE_SIZE_MB", "50")
		ds := svnConfig(t, "http://127.0.0.1/repo", nil)
		ds.Settings["max_file_size_mb"] = float64(8) // JSON numbers decode to float64
		cfg, err := parseConfig(ds)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if cfg.MaxFileBytes != 8*mb {
			t.Fatalf("want the narrowed limit, got %d", cfg.MaxFileBytes)
		}
	})

	t.Run("a larger setting cannot outrun the platform", func(t *testing.T) {
		// Fetching more than the ingest path can parse only wastes bandwidth and
		// fills the sync log with parser failures, so the deployment wins.
		t.Setenv("MAX_FILE_SIZE_MB", "50")
		ds := svnConfig(t, "http://127.0.0.1/repo", nil)
		ds.Settings["max_file_size_mb"] = "512"
		cfg, err := parseConfig(ds)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if cfg.MaxFileBytes != 50*mb {
			t.Fatalf("want the deployment limit, got %d", cfg.MaxFileBytes)
		}
	})

	t.Run("useless settings fall back to the deployment limit", func(t *testing.T) {
		t.Setenv("MAX_FILE_SIZE_MB", "50")
		for _, raw := range []interface{}{float64(0), float64(-5), float64(1.5), "abc", true, nil, ""} {
			ds := svnConfig(t, "http://127.0.0.1/repo", nil)
			ds.Settings["max_file_size_mb"] = raw
			cfg, err := parseConfig(ds)
			if err != nil {
				t.Fatalf("parse with %v: %v", raw, err)
			}
			if cfg.MaxFileBytes != 50*mb {
				t.Fatalf("raw %v: want the deployment limit, got %d", raw, cfg.MaxFileBytes)
			}
		}
	})

	t.Run("absurd deployment limits are capped", func(t *testing.T) {
		// Three in-memory copies of one file: above the ceiling the sync would
		// OOM the whole service instead of skipping a single file.
		t.Setenv("MAX_FILE_SIZE_MB", "4096")
		cfg, err := parseConfig(svnConfig(t, "http://127.0.0.1/repo", nil))
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if cfg.MaxFileBytes != maxSupportedFileBytes {
			t.Fatalf("want the supported ceiling, got %d", cfg.MaxFileBytes)
		}
	})
}

func TestSettingMegabytesAcceptsCommonShapes(t *testing.T) {
	cases := []struct {
		raw    interface{}
		want   int64
		wantOK bool
	}{
		{raw: float64(50), want: 50, wantOK: true},
		{raw: int(7), want: 7, wantOK: true},
		{raw: int64(9), want: 9, wantOK: true},
		{raw: json.Number("12"), want: 12, wantOK: true},
		{raw: "15", want: 15, wantOK: true},
		{raw: float64(0)},
		{raw: float64(-1)},
		{raw: float64(2.5)},
		{raw: "big"},
		{raw: nil},
		{raw: true},
	}
	for _, tc := range cases {
		got, ok := settingMegabytes(tc.raw)
		if ok != tc.wantOK || got != tc.want {
			t.Fatalf("settingMegabytes(%v) = (%d, %v), want (%d, %v)", tc.raw, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestDownloadTimeoutScalesWithFileSize(t *testing.T) {
	if got := downloadTimeout(-1); got != maxDownloadTimeout {
		t.Fatalf("unknown size should get the ceiling, got %v", got)
	}
	if got := downloadTimeout(1 << 20); got != downloadTimeoutBase+downloadBlockBudget {
		t.Fatalf("a partial block still costs a full block, got %v", got)
	}
	if got := downloadTimeout(3 * downloadBlockBytes); got != downloadTimeoutBase+3*downloadBlockBudget {
		t.Fatalf("three blocks should get three block budgets, got %v", got)
	}
	// A 400 MB file must not be judged by the old flat two-minute limit.
	if small, large := downloadTimeout(20<<20), downloadTimeout(400<<20); large <= small {
		t.Fatalf("a larger file needs a larger budget: %v vs %v", large, small)
	}
	if got := downloadTimeout(50 << 30); got != maxDownloadTimeout {
		t.Fatalf("an enormous file must stop at the ceiling, got %v", got)
	}
}

func TestReadCappedRejectsBodiesOverTheLimit(t *testing.T) {
	body := strings.Repeat("x", 4096)
	if _, err := readCapped(strings.NewReader(body), int64(len(body)), int64(len(body))); err != nil {
		t.Fatalf("a body at the limit must pass: %v", err)
	}
	_, err := readCapped(strings.NewReader(body), int64(len(body)-1), 0)
	var tooLarge errFileTooLarge
	if !errors.As(err, &tooLarge) {
		t.Fatalf("want errFileTooLarge, got %v", err)
	}
	if tooLarge.limit != int64(len(body)-1) {
		t.Fatalf("limit = %d", tooLarge.limit)
	}
}

func TestOversizedFileIsReportedWithoutAbortingTheSync(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "10")
	const bigSize = 300 << 20

	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 1})
	repo.put("trunk/notes.md", fakeNode{rev: 2, content: "notes"})
	// The listing advertises a 300 MB blob while the body stays tiny: the
	// connector must decide from the listing alone and never issue the GET.
	repo.put("trunk/dataset.pdf", fakeNode{rev: 3, content: "%PDF-stub", reportedSize: bigSize})
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)

	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("an oversized file must not fail the sync: %v", err)
	}
	byPath := itemsByPath(first.items)
	if got := string(byPath["trunk/notes.md"].Content); got != "notes" {
		t.Fatalf("the rest of the repository must still sync, got %q", got)
	}
	skipped, ok := byPath["trunk/dataset.pdf"]
	if !ok {
		t.Fatalf("the skipped file must be reported, got %v", itemPaths(first.items))
	}
	if len(skipped.Content) != 0 {
		t.Fatal("a skipped file must carry no content")
	}
	if skipped.URL != "" {
		t.Fatal("a skipped file must carry no URL: the engine would fetch it anyway")
	}
	if skipped.Metadata["error"] == "" || skipped.Metadata["error_reason_code"] != "file_too_large" {
		t.Fatalf("want a classified failure, got %+v", skipped.Metadata)
	}
	if !strings.Contains(skipped.Metadata["error"], "300 MB") {
		t.Fatalf("the reason should name the size, got %q", skipped.Metadata["error"])
	}
	if skipped.ExternalID != svnExternalID(baseURL, "trunk/dataset.pdf") {
		t.Fatalf("the failure must stay addressable, got %q", skipped.ExternalID)
	}
	for _, request := range repo.requestLog() {
		if strings.Contains(request, "dataset.pdf") && strings.HasPrefix(request, "GET ") {
			t.Fatalf("an oversized file must not be downloaded: %s", request)
		}
	}

	// A skipped file is recorded as skipped (not as synced content), so the run
	// does not report it again, while a raised limit still picks it up.
	state := decodeCursor(cursor)
	if got := state.Repositories[baseURL]["trunk/dataset.pdf"].SkipLimit; got != 10<<20 {
		t.Fatalf("a skipped file must be tracked with the limit that skipped it, got %d", got)
	}
	if _, tracked := state.Repositories[baseURL]["trunk/notes.md"]; !tracked {
		t.Fatal("an ingested file must be tracked")
	}

	// Same limit, nothing changed: the failure is not re-reported on every run.
	quiet := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, cursor, quiet); err != nil {
		t.Fatalf("repeat stream: %v", err)
	}
	if len(quiet.items) != 0 {
		t.Fatalf("an unchanged skip must not be reported again, got %v", itemPaths(quiet.items))
	}

	// Raise the deployment limit (the only supported way) and the file syncs.
	t.Setenv("MAX_FILE_SIZE_MB", "1024")
	second := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, cursor, second); err != nil {
		t.Fatalf("stream after raising the limit: %v", err)
	}
	raised := itemsByPath(second.items)
	if got := string(raised["trunk/dataset.pdf"].Content); got != "%PDF-stub" {
		t.Fatalf("the previously skipped file must be fetched, got %q", got)
	}
}

func TestOversizedDownloadIsReportedNotFatal(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "1")

	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 1})
	// big.md sorts first and hides its size, so the connector discovers the
	// oversize only after the GET; zzz.md then proves the sync continued.
	repo.put("trunk/big.md", fakeNode{rev: 2, content: strings.Repeat("x", (1<<20)+1024), omitSize: true})
	repo.put("trunk/zzz.md", fakeNode{rev: 3, content: "small"})
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	h := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), svnConfig(t, baseURL, nil), nil, h)
	if err != nil {
		t.Fatalf("a download that exceeds the limit must not fail the sync: %v", err)
	}
	byPath := itemsByPath(h.items)
	if byPath["trunk/big.md"].Metadata["error_reason_code"] != "file_too_large" {
		t.Fatalf("want a classified failure, got %+v", byPath["trunk/big.md"].Metadata)
	}
	if got := string(byPath["trunk/zzz.md"].Content); got != "small" {
		t.Fatalf("the sync must continue past the oversized file, got %q", got)
	}
	if got := decodeCursor(cursor).Repositories[baseURL]["trunk/big.md"].SkipLimit; got != 1<<20 {
		t.Fatalf("a download that overshot the limit must be recorded as skipped, got %d", got)
	}
}

func TestFullSyncAlsoReportsOversizedFiles(t *testing.T) {
	t.Setenv("MAX_FILE_SIZE_MB", "1")
	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 1})
	repo.put("trunk/keep.md", fakeNode{rev: 2, content: "keep"})
	repo.put("trunk/huge.pdf", fakeNode{rev: 3, content: "%PDF", reportedSize: 50 << 20})
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	h := &collectHandler{}
	if _, err := connector.FetchFullStream(context.Background(), svnConfig(t, baseURL, nil), nil, h); err != nil {
		t.Fatalf("full stream: %v", err)
	}
	byPath := itemsByPath(h.items)
	if got := string(byPath["trunk/keep.md"].Content); got != "keep" {
		t.Fatalf("full sync must still ingests the rest, got %q", got)
	}
	if byPath["trunk/huge.pdf"].Metadata["error_reason_code"] != "file_too_large" {
		t.Fatalf("full sync must report the skipped file, got %+v", byPath["trunk/huge.pdf"].Metadata)
	}
}

func TestLoweringTheLimitDoesNotDeleteAnIngestedFile(t *testing.T) {
	// A file that is already in the knowledge base and then falls outside a
	// narrowed limit must be reported as skipped, never as deleted: the
	// knowledge entry is stale content, not a removed file.
	t.Setenv("MAX_FILE_SIZE_MB", "50")
	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 1})
	repo.put("trunk/manual.pdf", fakeNode{rev: 2, content: "%PDF", reportedSize: 5 << 20})
	_, baseURL := newTestServer(t, repo)

	connector := NewConnector()
	ds := svnConfig(t, baseURL, nil)
	first := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, first)
	if err != nil {
		t.Fatalf("initial stream: %v", err)
	}
	if got := string(itemsByPath(first.items)["trunk/manual.pdf"].Content); got != "%PDF" {
		t.Fatalf("the file must be ingested while it fits, got %q", got)
	}

	ds.Settings["max_file_size_mb"] = float64(1)
	second := &collectHandler{}
	next, err := connector.FetchStream(context.Background(), ds, cursor, second)
	if err != nil {
		t.Fatalf("stream with a narrowed limit: %v", err)
	}
	for _, item := range second.items {
		if item.IsDeleted {
			t.Fatalf("narrowing the limit must not delete knowledge: %+v", item)
		}
	}
	skipped := itemsByPath(second.items)["trunk/manual.pdf"]
	if skipped.Metadata["error_reason_code"] != "file_too_large" {
		t.Fatalf("want a size failure, got %+v", skipped.Metadata)
	}
	if _, ok := decodeCursor(next).Repositories[baseURL]["trunk/manual.pdf"]; !ok {
		t.Fatal("the path must stay tracked so it is not reported as deleted")
	}
}

// abortingHandler records every checkpoint and aborts the walk (like the engine
// abandoning a sync when the Asynq task times out) as soon as an item under
// abortOn is emitted.
type abortingHandler struct {
	items   []types.FetchedItem
	last    *types.SyncCursor
	abortOn string
}

func (h *abortingHandler) Emit(_ context.Context, item types.FetchedItem) error {
	if h.abortOn != "" && strings.Contains(item.Metadata["svn_path"], h.abortOn) {
		return errors.New("simulated engine abort")
	}
	h.items = append(h.items, item)
	return nil
}

func (h *abortingHandler) Checkpoint(_ context.Context, cursor *types.SyncCursor) error {
	h.last = cursor
	return nil
}

// twoPathRepo declares two independent selections so a run can finish the first
// and be aborted inside the second.
func twoPathRepo() (*fakeRepo, []interface{}) {
	repo := newFakeRepo("/svn/repo")
	repo.put("trunk", fakeNode{dir: true, rev: 2})
	repo.put("trunk/a", fakeNode{dir: true, rev: 3})
	repo.put("trunk/a/one.md", fakeNode{rev: 4, content: "one"})
	repo.put("trunk/b", fakeNode{dir: true, rev: 3})
	repo.put("trunk/b/two.md", fakeNode{rev: 5, content: "two"})
	return repo, []interface{}{
		map[string]interface{}{"url": "", "paths": []interface{}{"trunk/a", "trunk/b"}},
	}
}

func roundTripCursor(t *testing.T, cursor *types.SyncCursor) *types.SyncCursor {
	t.Helper()
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

// A run aborted inside a repository (the Asynq task timeout, a shutdown) must
// checkpoint the selections it already finished and, on the retry, resume at
// the next selection instead of re-walking — and re-downloading — the finished
// ones.
func TestSyncResumesAtNextSelectionAfterMidRepositoryAbort(t *testing.T) {
	repo, repositories := twoPathRepo()
	_, baseURL := newTestServer(t, repo)
	repositories[0].(map[string]interface{})["url"] = baseURL

	connector := NewConnector()
	ds := svnConfig(t, baseURL, repositories)

	first := &abortingHandler{abortOn: "trunk/b"}
	if _, err := connector.FetchStream(context.Background(), ds, nil, first); err == nil {
		t.Fatal("the aborted walk must surface its error")
	}
	if first.last == nil {
		t.Fatal("finishing the first selection must have checkpointed progress")
	}
	midway := decodeCursor(first.last)
	progress, ok := midway.Pending[baseURL]
	if !ok {
		t.Fatalf("a repository stopped mid-walk must keep its progress, got %+v", midway.Pending)
	}
	if len(progress.Done) != 1 || progress.Done[0] != "trunk/a" {
		t.Fatalf("the finished selection must be recorded, got %v", progress.Done)
	}
	if _, committed := midway.Repositories[baseURL]["trunk/a/one.md"]; committed {
		t.Fatal("the repository must not be committed until every selection is walked")
	}

	// Resume through a JSON round trip, exactly as the data_sources jsonb
	// column stores and returns the cursor.
	resumed := &collectHandler{}
	next, err := connector.FetchStream(context.Background(), ds, roundTripCursor(t, first.last), resumed)
	if err != nil {
		t.Fatalf("resumed stream: %v", err)
	}
	if got := itemPaths(resumed.items); strings.Join(got, ",") != "trunk/b/two.md" {
		t.Fatalf("resume must walk only the unfinished selection, got %v", got)
	}
	for _, item := range resumed.items {
		if item.IsDeleted {
			t.Fatalf("resume must not delete the selection already walked: %+v", item)
		}
	}
	final := decodeCursor(next)
	if len(final.Pending) != 0 {
		t.Fatalf("a completed repository must clear its pending progress, got %+v", final.Pending)
	}
	if _, ok := final.Repositories[baseURL]["trunk/a/one.md"]; !ok {
		t.Fatal("the finished selection's files must be committed")
	}
	if _, ok := final.Repositories[baseURL]["trunk/b/two.md"]; !ok {
		t.Fatal("the resumed selection's files must be committed")
	}
}

// The resumed run must re-download nothing from the selection it already
// finished: the checkpointed working set stands in for a re-walk.
func TestSyncResumeDoesNotRedownloadFinishedSelection(t *testing.T) {
	repo, repositories := twoPathRepo()
	_, baseURL := newTestServer(t, repo)
	repositories[0].(map[string]interface{})["url"] = baseURL

	connector := NewConnector()
	ds := svnConfig(t, baseURL, repositories)

	first := &abortingHandler{abortOn: "trunk/b"}
	if _, err := connector.FetchStream(context.Background(), ds, nil, first); err == nil {
		t.Fatal("the aborted walk must surface its error")
	}
	before := repo.requestLog()

	resumed := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, roundTripCursor(t, first.last), resumed); err != nil {
		t.Fatalf("resumed stream: %v", err)
	}
	for _, request := range repo.requestLog()[len(before):] {
		if strings.HasPrefix(request, "GET ") && strings.Contains(request, "one.md") {
			t.Fatalf("the finished selection must not be fetched again: %s", request)
		}
	}
	if _, ok := itemsByPath(resumed.items)["trunk/b/two.md"]; !ok {
		t.Fatalf("the unfinished selection must still sync, got %v", itemPaths(resumed.items))
	}
}

// Deletions are reconciled against the last committed baseline, so a file
// removed from a selection that a prior aborted run had already walked is still
// reported once the repository finishes on a later attempt.
func TestResumedSyncReconcilesDeletionsAgainstCommittedBaseline(t *testing.T) {
	repo, repositories := twoPathRepo()
	_, baseURL := newTestServer(t, repo)
	repositories[0].(map[string]interface{})["url"] = baseURL

	connector := NewConnector()
	ds := svnConfig(t, baseURL, repositories)

	seed := &collectHandler{}
	cursor, err := connector.FetchStream(context.Background(), ds, nil, seed)
	if err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	// trunk/a/one.md is gone, and trunk/b/two.md changes so the second
	// selection emits and the run can be aborted there.
	repo.remove("trunk/a/one.md")
	repo.put("trunk/b/two.md", fakeNode{rev: 99, content: "two v2"})

	first := &abortingHandler{abortOn: "trunk/b"}
	if _, err := connector.FetchStream(context.Background(), ds, cursor, first); err == nil {
		t.Fatal("the aborted walk must surface its error")
	}
	if first.last == nil {
		t.Fatal("finishing trunk/a must have checkpointed progress")
	}

	resumed := &collectHandler{}
	if _, err := connector.FetchStream(context.Background(), ds, roundTripCursor(t, first.last), resumed); err != nil {
		t.Fatalf("resumed stream: %v", err)
	}
	byPath := itemsByPath(resumed.items)
	deletion, ok := byPath["trunk/a/one.md"]
	if !ok || !deletion.IsDeleted {
		t.Fatalf("a file removed before the abort must still be reconciled, got %v", itemPaths(resumed.items))
	}
	if got := string(byPath["trunk/b/two.md"].Content); got != "two v2" {
		t.Fatalf("the resumed selection must sync its change, got %q", got)
	}
}
