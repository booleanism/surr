#!/usr/bin/env bash
#
# Cut a release: tag the root module, then pin and tag the five service modules.
#
# The order is not a style choice. A service module cannot resolve
# `github.com/booleanism/surr vX.Y.Z` until that version exists on the proxy, and
# `go install ...@version` refuses a module whose go.mod carries a `replace`
# directive -- so the service go.sum entries can only be produced *after* the root
# tag is pushed, and the service tags can only be placed *after* those entries
# exist. Doing it in any other order leaves either a module that will not tidy or
# a tag that will not install.
#
#   usage: ./scripts/release.sh v0.2.0 [--dry-run]
#
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODULES=(hub subscriptions analytics rx tx)

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

for tag in "$VERSION" $(for m in "${MODULES[@]}"; do echo "$m/$VERSION"; done); do
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        die "tag $tag already exists locally"
    fi
    if git ls-remote --tags --exit-code origin "refs/tags/$tag" >/dev/null 2>&1; then
        die "tag $tag already exists on origin"
    fi
done

echo "==> gates (make) before tagging"
run make

# --- phase 1: the root module ------------------------------------------------
#
# Tagged on the current commit rather than on the pinning commit below: the
# release commit touches only the service go.mod/go.sum files, so the root
# module's content is identical either way, and tagging it here is what makes
# phase 2 resolvable.

echo "==> tagging the root module $VERSION"
run git tag -a "$VERSION" -m "Release $VERSION"
run git push origin "$VERSION"

# --- phase 2: pin and tag the service modules --------------------------------

echo "==> pinning the service modules to $VERSION"
for m in "${MODULES[@]}"; do
    echo "  -> $m"
    # Both edits in one call: a require without dropping the replace is exactly
    # the state go install refuses, and it is the state a half-finished release
    # leaves behind.
    run go mod edit \
        -dropreplace="github.com/booleanism/surr" \
        -require="github.com/booleanism/surr@$VERSION" \
        "./$m/go.mod"
    # GOWORK=off, or the workspace answers from the local directory and no go.sum
    # entry is written -- the pin would then be unverifiable outside a checkout.
    if [ "$DRY_RUN" = 0 ]; then
        ( cd "$m" && GOWORK=off go mod tidy )
        ( cd "$m" && GOWORK=off go build ./... )
    fi
done

echo "==> committing the pins"
FILES=()
for m in "${MODULES[@]}"; do FILES+=("$m/go.mod" "$m/go.sum"); done
run git add -- "${FILES[@]}" go.work.sum
run git commit -m "release: pin the service modules to $VERSION"

echo "==> tagging the service modules"
for m in "${MODULES[@]}"; do
    run git tag -a "$m/$VERSION" -m "Release $m $VERSION"
done
run git push origin main
for m in "${MODULES[@]}"; do
    run git push origin "$m/$VERSION"
done

echo
echo "released $VERSION. verify from a clean cache with:"
echo "  GOMODCACHE=\$(mktemp -d) go install github.com/booleanism/surr/hub/cmd/hub@$VERSION"
