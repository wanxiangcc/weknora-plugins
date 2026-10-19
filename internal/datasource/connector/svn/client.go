package svn

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/datasource"
)

const (
	// svnDAVNamespace is mod_dav_svn's own property namespace
	// (SVN_DAV_PROP_NS_DAV in Subversion's svn_dav.h). Properties such as
	// baseline-relative-path and md5-checksum live there; the generic DAV
	// properties live in "DAV:".
	svnDAVNamespace = "http://subversion.tigris.org/xmlns/dav/"

	// maxPropfindBytes bounds one PROPFIND multistatus body. A Depth: 1 listing
	// of a very large directory is legitimately big, but an unbounded body lets
	// a broken or hostile server exhaust process memory.
	maxPropfindBytes int64 = 64 << 20

	// listTimeout covers one PROPFIND: a listing is small, so a slow answer means
	// a broken server and the operator should hear about it quickly.
	listTimeout = 120 * time.Second

	// File downloads are bounded per request instead of by a single constant: a
	// repository file can be hundreds of megabytes, and the old flat 120s budget
	// turned a slow-but-healthy transfer into a sync failure. The budget grows
	// with the reported size (downloadBlockBytes per downloadBlockBudget) and is
	// capped, so a stalled connection still fails in bounded time.
	downloadTimeoutBase   = 2 * time.Minute
	downloadBlockBytes    = 20 << 20
	downloadBlockBudget   = time.Minute
	maxDownloadTimeout    = 30 * time.Minute
	preallocatedReadSlack = 1 << 20

	// Transport-level failures (a dropped connection, EOF, a timeout) are
	// routinely transient on a busy mod_dav_svn that resets connections under
	// load. Each request is retried a few times with linear backoff before the
	// failure is surfaced, so a single blip does not fail the whole sync.
	// HTTP status errors are never retried here.
	transportAttempts = 3
	transportBackoff  = 300 * time.Millisecond
)

// isTransientTransportError reports whether err is a transport failure a bounded
// retry can plausibly recover from. HTTP status errors (apiError) and credential
// failures are deliberately excluded: retrying them cannot change the outcome.
func isTransientTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	// net.Error covers *net.OpError (connection reset/refused), DNS failures and
	// *url.Error timeouts; the latter also wraps a per-request context deadline.
	var netErr net.Error
	return errors.As(err, &netErr)
}

// retryTransport runs fn, retrying transport errors up to transportAttempts
// times with linear backoff. It stops early when the caller's context is done,
// so a cancelled sync is not held open by the backoff.
func retryTransport(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < transportAttempts; attempt++ {
		if err = fn(); err == nil || !isTransientTransportError(err) {
			return err
		}
		if ctx.Err() != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(transportBackoff * time.Duration(attempt+1)):
		}
	}
	return err
}

// errFileTooLarge reports a file the connector refuses to fetch. It is a
// distinct type (rather than a plain error) because an oversized file is a
// per-file condition: the connector records it as one failed item and keeps
// syncing, instead of aborting the whole repository.
type errFileTooLarge struct {
	limit int64
	// size is the offending size, or -1 when the server never reported one.
	size int64
}

func (e errFileTooLarge) Error() string {
	if e.size >= 0 {
		return fmt.Sprintf("file is %s, above the %s per-file limit", humanBytes(e.size), humanBytes(e.limit))
	}
	return fmt.Sprintf("file is above the %s per-file limit", humanBytes(e.limit))
}

// humanBytes renders a byte count the way the sync log should show it: whole
// megabytes when it divides evenly, one decimal otherwise.
func humanBytes(n int64) string {
	const mb = 1 << 20
	if n >= mb && n%mb == 0 {
		return fmt.Sprintf("%d MB", n/mb)
	}
	if n >= mb {
		return fmt.Sprintf("%.1f MB", float64(n)/float64(mb))
	}
	return fmt.Sprintf("%d KB", n/(1<<10))
}

// downloadTimeout is the time budget for one file body, scaled by the size the
// server reported in the listing. An unknown size gets the full cap: it is
// better to wait than to fail a large file whose length the server omitted.
func downloadTimeout(size int64) time.Duration {
	if size <= 0 {
		return maxDownloadTimeout
	}
	blocks := size / downloadBlockBytes
	if size%downloadBlockBytes != 0 {
		blocks++
	}
	// Guard the multiplication: a broken server can report a nonsense length.
	if blocks > int64(maxDownloadTimeout/downloadBlockBudget) {
		return maxDownloadTimeout
	}
	budget := downloadTimeoutBase + time.Duration(blocks)*downloadBlockBudget
	if budget > maxDownloadTimeout {
		return maxDownloadTimeout
	}
	return budget
}

// propfindBody asks for exactly the properties the connector consumes. The
// element names are matched by local name when parsing, so a server that
// answers with a slightly different prefix still works.
const propfindBody = `<?xml version="1.0" encoding="utf-8"?>
<D:propfind xmlns:D="DAV:" xmlns:S="` + svnDAVNamespace + `">
  <D:prop>
    <D:resourcetype/>
    <D:getcontentlength/>
    <D:getlastmodified/>
    <D:getetag/>
    <D:getcontenttype/>
    <D:version-name/>
    <D:creationdate/>
    <S:baseline-relative-path/>
    <S:md5-checksum/>
  </D:prop>
</D:propfind>`

// davEntry is one resource from a PROPFIND multistatus response.
type davEntry struct {
	// Href is the absolute URL of the resource, as reported by the server.
	Href string
	// RelPath is the resource path relative to the collection that was
	// requested (slash separated, no leading slash). Empty for the requested
	// collection itself.
	RelPath string
	// Collection reports whether the resource is a directory.
	Collection bool
	// Rev is the node's last-changed revision (the change signal for files).
	Rev string
	// Size is the reported content length; -1 when the server omits it.
	Size int64
	// ETag and MD5 come from the server when available and back up Rev on
	// servers that do not expose version-name.
	ETag string
	MD5  string
	// Modified is the node's last-changed time when the server reports one.
	Modified time.Time
	// RepoPath is mod_dav_svn's repository-relative path, used for display and
	// PR URLs; informational only.
	RepoPath string
}

type apiError struct {
	endpoint string
	status   int
}

func (e *apiError) Error() string {
	return fmt.Sprintf("svn webdav %s: status %d", e.endpoint, e.status)
}

type client struct {
	http         *http.Client
	username     string
	password     string
	maxFileBytes int64
}

func newClient(username, password string, maxFileBytes int64) *client {
	return &client{
		// The client timeout is only a ceiling; each request narrows it with its
		// own context deadline (listTimeout for PROPFIND, downloadTimeout(size)
		// for a file), so a slow listing never inherits a download's budget.
		http:         datasource.NewConnectorHTTPClient(maxDownloadTimeout),
		username:     username,
		password:     password,
		maxFileBytes: maxFileBytes,
	}
}

func (c *client) authorize(req *http.Request) {
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}
}

// readCapped reads a body, refusing anything larger than limit instead of
// buffering it. A truncated body would be indexed as if it were the whole
// document, so oversized payloads are an error. sizeHint (usually the reported
// content length) pre-grows the buffer, which avoids the repeated reallocation
// io.ReadAll would do for a multi-hundred-megabyte file.
func readCapped(body io.Reader, limit, sizeHint int64) ([]byte, error) {
	var buf bytes.Buffer
	if sizeHint > 0 && sizeHint <= limit+preallocatedReadSlack {
		buf.Grow(int(sizeHint))
	}
	written, err := io.Copy(&buf, io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if written > limit {
		return nil, errFileTooLarge{limit: limit, size: written}
	}
	return buf.Bytes(), nil
}

// propfind lists one collection at the given DAV Depth ("0" or "1").
func (c *client) propfind(ctx context.Context, collectionURL, depth string) ([]davEntry, error) {
	var entries []davEntry
	err := retryTransport(ctx, func() error {
		var attemptErr error
		entries, attemptErr = c.propfindOnce(ctx, collectionURL, depth)
		return attemptErr
	})
	return entries, err
}

func (c *client) propfindOnce(ctx context.Context, collectionURL, depth string) ([]davEntry, error) {
	reqCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		reqCtx, "PROPFIND", collectionURL, strings.NewReader(propfindBody),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Depth", depth)
	req.Header.Set("Content-Type", "application/xml; charset=utf-8")
	c.authorize(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: svn webdav %s: status %d", datasource.ErrInvalidCredentials, collectionURL, resp.StatusCode)
	}
	if resp.StatusCode != http.StatusMultiStatus {
		return nil, &apiError{endpoint: collectionURL, status: resp.StatusCode}
	}
	body, err := readCapped(resp.Body, maxPropfindBytes, resp.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("svn webdav %s: %w", collectionURL, err)
	}
	return parseMultiStatus(body, collectionURL)
}

// get downloads one file. The connector always reads the repository's current
// revision: the knowledge entry is keyed by path, not by revision, so a commit
// landing mid-sync is corrected by the next run.
//
// sizeHint is the length the listing reported, used both to size the time
// budget and to reject a file that is already known to be oversized before a
// single byte of its body is read.
func (c *client) get(ctx context.Context, fileURL string, sizeHint int64) ([]byte, error) {
	if sizeHint > c.maxFileBytes {
		return nil, errFileTooLarge{limit: c.maxFileBytes, size: sizeHint}
	}
	var body []byte
	err := retryTransport(ctx, func() error {
		var attemptErr error
		body, attemptErr = c.getOnce(ctx, fileURL, sizeHint)
		return attemptErr
	})
	return body, err
}

func (c *client) getOnce(ctx context.Context, fileURL string, sizeHint int64) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, downloadTimeout(sizeHint))
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: svn webdav %s: status %d", datasource.ErrInvalidCredentials, fileURL, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &apiError{endpoint: fileURL, status: resp.StatusCode}
	}
	// Trust the response over the listing: a file can grow between the PROPFIND
	// and the GET, and a server may send an unsolicited content length.
	declared := resp.ContentLength
	if declared <= 0 {
		declared = sizeHint
	}
	if declared > c.maxFileBytes {
		return nil, errFileTooLarge{limit: c.maxFileBytes, size: declared}
	}
	body, err := readCapped(resp.Body, c.maxFileBytes, declared)
	if err != nil {
		var tooLarge errFileTooLarge
		if errors.As(err, &tooLarge) {
			return nil, tooLarge
		}
		return nil, fmt.Errorf("svn file %s: %w", fileURL, err)
	}
	return body, nil
}

type multiStatus struct {
	Responses []davResponse `xml:"response"`
}

type davResponse struct {
	Href     string        `xml:"href"`
	Propstat []davPropstat `xml:"propstat"`
}

type davPropstat struct {
	Prop   davProp `xml:"prop"`
	Status string  `xml:"status"`
}

type davCollection struct{}

type davProp struct {
	ResourceType         davResourceType `xml:"resourcetype"`
	ContentLength        string          `xml:"getcontentlength"`
	GetLastModified      string          `xml:"getlastmodified"`
	GetETag              string          `xml:"getetag"`
	CreationDate         string          `xml:"creationdate"`
	VersionName          string          `xml:"version-name"`
	BaselineRelativePath string          `xml:"baseline-relative-path"`
	MD5Checksum          string          `xml:"md5-checksum"`
}

type davResourceType struct {
	Collection *davCollection `xml:"collection"`
}

func parseMultiStatus(body []byte, collectionURL string) ([]davEntry, error) {
	var ms multiStatus
	if err := xml.Unmarshal(body, &ms); err != nil {
		return nil, fmt.Errorf("svn webdav %s: malformed multistatus: %w", collectionURL, err)
	}
	base, err := url.Parse(collectionURL)
	if err != nil {
		return nil, err
	}
	out := make([]davEntry, 0, len(ms.Responses))
	for _, response := range ms.Responses {
		prop, ok := usableProp(response.Propstat)
		if !ok {
			// Every requested property came back as an error (for example a
			// server that does not implement version-name AND resourcetype).
			// Skipping keeps a partial listing usable instead of failing the
			// whole sync on one exotic node.
			continue
		}
		relPath, err := relativePath(base, response.Href)
		if err != nil {
			// mod_dav_svn always reports hrefs under the requested collection;
			// anything else means the server is misconfigured or a proxy
			// rewrote the URL, and silently mis-keying files would be worse than
			// reporting it.
			return nil, fmt.Errorf("svn webdav %s: %w", collectionURL, err)
		}
		href, err := absoluteHref(base, response.Href)
		if err != nil {
			return nil, fmt.Errorf("svn webdav %s: %w", collectionURL, err)
		}
		entry := davEntry{
			Href:       href,
			RelPath:    relPath,
			Collection: prop.ResourceType.Collection != nil,
			Rev:        strings.TrimSpace(prop.VersionName),
			Size:       -1,
			ETag:       strings.Trim(strings.TrimSpace(prop.GetETag), "\""),
			MD5:        strings.TrimSpace(prop.MD5Checksum),
			Modified:   parseDAVTime(prop.CreationDate, prop.GetLastModified),
			RepoPath:   strings.Trim(strings.TrimSpace(prop.BaselineRelativePath), "/"),
		}
		if size, err := strconv.ParseInt(strings.TrimSpace(prop.ContentLength), 10, 64); err == nil {
			entry.Size = size
		}
		out = append(out, entry)
	}
	return out, nil
}

// usableProp returns the properties of the first successful propstat block.
// DAV servers may split a response into one 200 block and one 404 block listing
// the properties they do not implement; only the 200 block carries data.
func usableProp(stats []davPropstat) (davProp, bool) {
	for _, stat := range stats {
		status := strings.TrimSpace(stat.Status)
		if status == "" || strings.Contains(status, " 200 ") || strings.HasSuffix(status, " 200") {
			return stat.Prop, true
		}
	}
	return davProp{}, false
}

// relativePath maps a multistatus href onto the path it occupies under the
// requested collection.
func relativePath(base *url.URL, href string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return "", fmt.Errorf("invalid href %q", href)
	}
	target := decodedPath(parsed.Path)
	if target == "" {
		target = decodedPath(parsed.Opaque)
	}
	basePath := strings.TrimRight(decodedPath(base.Path), "/")
	if basePath == "" {
		basePath = "/"
	}
	target = strings.TrimRight(target, "/")
	switch {
	case target == basePath:
		return "", nil
	case strings.HasPrefix(target, basePath+"/"):
		return strings.TrimPrefix(target, basePath+"/"), nil
	}
	// Fallback for front ends that answer with a href relative to some outer
	// mount point: locate the requested collection's own segment and keep
	// everything below it. Anything else is rejected rather than guessed,
	// because a wrong relpath would mis-key every file below it.
	marker := "/" + pathBase(basePath) + "/"
	if idx := strings.LastIndex(target, marker); idx >= 0 {
		return target[idx+len(marker):], nil
	}
	return "", fmt.Errorf("href %q is outside requested collection %q", href, basePath)
}

// decodedPath percent-decodes a URL path, tolerating servers that already send
// decoded bytes.
func decodedPath(value string) string {
	if decoded, err := url.PathUnescape(value); err == nil {
		return decoded
	}
	return value
}

func pathBase(p string) string {
	trimmed := strings.TrimRight(p, "/")
	if idx := strings.LastIndex(trimmed, "/"); idx >= 0 {
		return trimmed[idx+1:]
	}
	return trimmed
}

// absoluteHref turns a possibly path-only href into an absolute URL so it can
// be used for the next PROPFIND or for a GET.
func absoluteHref(base *url.URL, href string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return "", fmt.Errorf("invalid href %q", href)
	}
	return base.ResolveReference(parsed).String(), nil
}

// parseDAVTime accepts the two timestamp shapes DAV servers use: RFC 3339
// (creationdate) and HTTP dates (getlastmodified). Unparseable values yield the
// zero time, which the connector reports as "source time unknown" rather than
// fabricating a fetch time.
func parseDAVTime(values ...string) time.Time {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		for _, layout := range []string{time.RFC3339, http.TimeFormat, time.RFC1123, time.RFC1123Z, "2006-01-02T15:04:05Z"} {
			if parsed, err := time.Parse(layout, value); err == nil {
				return parsed.UTC()
			}
		}
	}
	return time.Time{}
}

// isPathGone reports whether an error means the requested repository path is
// not there. mod_dav_svn answers a PROPFIND for a deleted, renamed or never
// created path with 404, and 410 is the equivalent "gone" answer some front ends
// use.
//
// It is deliberately narrow. A 403 or a 5xx means the listing failed rather than
// that the path is absent, and treating those as absent would silently drop a
// subtree from the sync; they must keep failing loudly.
func isPathGone(err error) bool {
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.status == http.StatusNotFound || apiErr.status == http.StatusGone
}

// isNotImplemented reports whether an error came back as an unsupported
// operation, used to give the operator a clearer message than "status 405".
func isNotImplemented(err error) bool {
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.status == http.StatusMethodNotAllowed ||
		apiErr.status == http.StatusNotImplemented
}
