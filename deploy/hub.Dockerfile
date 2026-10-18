# The control plane: HTTP API (JWT auth, users, nodes, subscriptions) backed by
# SQLite, plus the RabbitMQ handlers.
#
# Build context is the repository ROOT (see .dockerignore) because hub/go.mod
# carries `replace github.com/booleanism/surr => ../`: the shared internal/
# tree has to sit beside the module for the build to resolve it.
#
# CGO is required and deliberate -- mattn/go-sqlite3 carries the SQLite
# amalgamation, so no system sqlite is needed, but it is not pure Go. The
# migration image (migrate.Dockerfile) links the same driver, which is what
# keeps the schema writer and the schema reader on one engine.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS build

# Injected at link time so the binary can name its own build. The same value is
# compiled into every service and is what `--version` and the startup log line
# report; the Makefile passes `git describe` here for local builds.
ARG VERSION=unknown

WORKDIR /src

COPY go.mod go.sum ./
COPY internal/ internal/
COPY hub/ hub/

WORKDIR /src/hub

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /out/surr-hub ./cmd/hub

FROM debian:trixie-slim

# apt runs against BuildKit cache mounts, so the .deb files and the package
# index survive a rebuild instead of being downloaded again. That is also why
# there is no `rm -rf /var/lib/apt/lists/*` here any more: the mounts never
# reach the image, so the cleanup would only wipe the cache. `sharing=locked`
# stops two concurrent builds from corrupting the shared lists.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr \
 && install -d -o surr -g surr /data

COPY --from=build /out/surr-hub /usr/local/bin/surr-hub

USER surr

WORKDIR /home/surr

EXPOSE 9000

ENTRYPOINT ["surr-hub"]
CMD ["run"]
