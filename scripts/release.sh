#!/usr/bin/env bash
#
# Cut a release: run the gates, tag, push.
#
# One tag is all it takes now that the repository is a single module. It used to
# be six, with an order that could not be permuted -- a service module cannot
# resolve `github.com/booleanism/surr vX.Y.Z` until that version is on the proxy,
# and `go install ...@version` refuses a module whose go.mod carries a `replace`
# directive. Collapsing the services into the root module removed both problems:
# there is no cross-module requirement to pin and no replace to drop.
#
#   usage: ./scripts/release.sh v0.3.0 [--dry-run]
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

VERSION="${1:-}"
DRY_RUN=0
[ "${2:-}" = "--dry-run" ] && DRY_RUN=1

die() { echo "error: $*" >&2; exit 1; }
run() {
    if [ "$DRY_RUN" = 1 ]; then
        echo "  would: $*"
    else
        "$@"
    fi
}

[ -n "$VERSION" ] || die "usage: $0 vX.Y.Z [--dry-run]"
echo "$VERSION" | grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$' \
    || die "version must look like vX.Y.Z, got '$VERSION'"

cd "$ROOT_DIR"

# --- preconditions -----------------------------------------------------------

[ -z "$(git status --porcelain)" ] || die "working tree is not clean"
[ "$(git rev-parse --abbrev-ref HEAD)" = "main" ] || die "not on main"

if git rev-parse -q --verify "refs/tags/$VERSION" >/dev/null; then
    die "tag $VERSION already exists locally"
fi
if git ls-remote --tags --exit-code origin "refs/tags/$VERSION" >/dev/null 2>&1; then
    die "tag $VERSION already exists on origin"
fi

echo "==> gates (make) before tagging"
run make

# --- tag ---------------------------------------------------------------------
#
# Annotated, not lightweight: `git describe` prefers annotated tags, and the
# Makefile feeds its output into VERSION, so a lightweight tag here would make
# every subsequent build report a bare commit hash.

echo "==> tagging $VERSION"
run git tag -a "$VERSION" -m "Release $VERSION"
run git push origin "$VERSION"

echo
echo "released $VERSION. verify from a clean cache with:"
for cmd in hub rx tx subscriptions analytics; do
    echo "  GOMODCACHE=\$(mktemp -d) go install github.com/booleanism/surr/$cmd/cmd/$cmd@$VERSION"
done
