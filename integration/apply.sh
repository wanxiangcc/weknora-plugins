#!/usr/bin/env bash
#
# Apply the SVN + Seeyon OA connectors to a WeKnora checkout.
#
#   ./apply.sh /path/to/WeKnora [--backend-only] [--dry-run]
#
# The script copies internal/datasource/connector/{svn,seeyon} and applies the
# patches in ./patches. It never commits, never restarts a service and never
# touches credentials.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: apply.sh <WeKnora-checkout> [options]

Options:
  --backend-only   Copy the connectors and apply only the backend patches
                   (0001-0003); leave the frontend untouched.
  --dry-run        Check that every patch applies, but change nothing.
  -h, --help       Show this help.

The target must be a WeKnora checkout (a directory with the
github.com/Tencent/WeKnora go.mod). Patches are generated against v0.8.2
(commit a64645b4); on a newer tree they may need manual alignment.
USAGE
}

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
warn() { printf 'warning: %s\n' "$*" >&2; }

TARGET=""
BACKEND_ONLY=0
DRY_RUN=0

for arg in "$@"; do
  case "$arg" in
    --backend-only) BACKEND_ONLY=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    -*) die "unknown option: $arg (see --help)" ;;
    *)
      [ -z "$TARGET" ] || die "only one target directory is accepted"
      TARGET="$arg"
      ;;
  esac
done

[ -n "$TARGET" ] || { usage; exit 2; }
[ -d "$TARGET" ] || die "target directory does not exist: $TARGET"
TARGET="$(cd "$TARGET" && pwd)"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ -f "$TARGET/go.mod" ] || die "$TARGET is not a WeKnora checkout (no go.mod)"
grep -q '^module github.com/Tencent/WeKnora' "$TARGET/go.mod" \
  || die "$TARGET/go.mod does not declare module github.com/Tencent/WeKnora"

IS_GIT=0
# A linked worktree has a `.git` *file*, so ask git instead of testing for a
# directory.
if git -C "$TARGET" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  IS_GIT=1
fi

CONNECTORS=(svn seeyon)
PATCHES=(
  0001-core-connector-api.patch
  0002-service-and-handler.patch
  0003-registry.patch
  0004-frontend.patch
  0005-frontend-icon.patch
)
if [ "$BACKEND_ONLY" = 1 ]; then
  PATCHES=(
    0001-core-connector-api.patch
    0002-service-and-handler.patch
    0003-registry.patch
  )
fi

for name in "${CONNECTORS[@]}"; do
  src="$REPO_ROOT/internal/datasource/connector/$name"
  [ -d "$src" ] || die "missing connector sources: $src"
done
for patch in "${PATCHES[@]}"; do
  [ -f "$REPO_ROOT/integration/patches/$patch" ] \
    || die "missing patch: integration/patches/$patch"
done

apply_patch() {
  local patch="$1"
  local path="$REPO_ROOT/integration/patches/$patch"

  if [ "$DRY_RUN" = 1 ]; then
    if [ "$IS_GIT" = 1 ]; then
      git -C "$TARGET" apply --check "$path" \
        || die "$patch does not apply cleanly (dry run)"
    else
      ( cd "$TARGET" && patch -p1 --dry-run --forward <"$path" >/dev/null ) \
        || die "$patch does not apply cleanly (dry run)"
    fi
    printf 'would apply %s\n' "$patch"
    return
  fi

  if [ "$IS_GIT" = 1 ]; then
    if git -C "$TARGET" apply --whitespace=nowarn "$path" 2>/dev/null; then
      printf 'applied %s\n' "$patch"
      return
    fi
    # The 3-way merge helps when upstream moved the surrounding lines and the
    # base blobs are present in the target repository.
    if git -C "$TARGET" apply -3 --whitespace=nowarn "$path"; then
      printf 'applied %s (3-way)\n' "$patch"
      return
    fi
    die "$patch failed to apply; resolve it manually (see integration/README.md) and re-run with the remaining patches"
  fi

  ( cd "$TARGET" && patch -p1 --forward --fuzz=3 <"$path" ) \
    || die "$patch failed to apply; resolve it manually (see integration/README.md)"
  printf 'applied %s\n' "$patch"
}

printf 'WeKnora checkout: %s\n' "$TARGET"
printf 'Plugin root:      %s\n' "$REPO_ROOT"
[ "$DRY_RUN" = 1 ] && printf 'Mode:             dry run (nothing will be written)\n'

if [ -d "$TARGET/internal/datasource/connector/seeyon" ] && [ "$DRY_RUN" = 0 ]; then
  warn "internal/datasource/connector/seeyon already exists and will be overwritten"
fi
if [ "$BACKEND_ONLY" = 0 ] && [ ! -d "$TARGET/frontend" ]; then
  warn "$TARGET has no frontend directory; the frontend patches will fail (use --backend-only)"
fi

if [ "$DRY_RUN" = 0 ]; then
  for name in "${CONNECTORS[@]}"; do
    src="$REPO_ROOT/internal/datasource/connector/$name"
    dst="$TARGET/internal/datasource/connector/$name"
    mkdir -p "$dst"
    cp -R "$src/." "$dst/"
    printf 'copied   internal/datasource/connector/%s\n' "$name"
  done
fi

for patch in "${PATCHES[@]}"; do
  apply_patch "$patch"
done

if [ "$DRY_RUN" = 1 ]; then
  printf '\nDry run finished: every patch applies cleanly.\n'
  exit 0
fi

cat <<NEXT

Next steps:
  cd "$TARGET"
  go build ./internal/...
  go test ./internal/datasource/connector/svn/... ./internal/datasource/connector/seeyon/...
  go test ./internal/container/ -run TestConnector
  pnpm --dir frontend type-check    # if the frontend was patched

Then restart the WeKnora backend/web service and create the data source under
Knowledge Base -> Data Sources. See docs/svn.md and docs/seeyon.md.
NEXT
