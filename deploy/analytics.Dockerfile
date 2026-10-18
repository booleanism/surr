# The analytics service: the read path over MongoDB, plus the registry of the
# queries each node declared about its own documents.
#
# Build context is the repository ROOT (see .dockerignore) because
# analytics/go.mod carries `replace github.com/booleanism/surr => ../`: the
# shared internal/ tree has to sit beside the module for the build to resolve
# it.
#
# CGO is required and deliberate, exactly as in hub.Dockerfile: the declaration
# registry is SQLite through mattn/go-sqlite3, which carries the amalgamation but
# is not pure Go. Mongo needs nothing from the image -- the driver is pure Go.
#
# This image is also the first place in the stack that reads Mongo without the
# node's credentials: it takes its own --mongo-url, which is what makes the
# read-only split of TODO.md B5 possible without moving code.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS build

# Injected at link time so the binary can name its own build. The same value is
# compiled into every service and is what `--version` and the startup log line
# report; the Makefile passes `git describe` here for local builds.
ARG VERSION=unknown

WORKDIR /src

COPY go.mod go.sum ./
COPY internal/ internal/
COPY analytics/ analytics/

WORKDIR /src/analytics

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /out/surr-analytics ./cmd/analytics

FROM debian:trixie-slim

# apt runs against BuildKit cache mounts, so the package index survives a
# rebuild. `sharing=locked` stops two concurrent builds from corrupting it.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr \
 && install -d -o surr -g surr /data

COPY --from=build /out/surr-analytics /usr/local/bin/surr-analytics

USER surr

WORKDIR /home/surr

ENTRYPOINT ["surr-analytics"]
CMD ["run"]
