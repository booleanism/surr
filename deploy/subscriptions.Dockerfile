# Answers "which nodes belong to this subscription" from its own SQLite
# database. No HTTP surface -- it is a pure broker peer.
#
# Same shape as hub.Dockerfile: root build context (the `replace ... => ../`
# directive), CGO for mattn/go-sqlite3.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS build

ARG VERSION=unknown

WORKDIR /src

COPY go.mod go.sum ./
COPY internal/ internal/
COPY subscriptions/ subscriptions/

WORKDIR /src/subscriptions

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /out/surr-subscriptions ./cmd/subscriptions

FROM debian:trixie-slim

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr \
 && install -d -o surr -g surr /data

COPY --from=build /out/surr-subscriptions /usr/local/bin/surr-subscriptions

USER surr

WORKDIR /home/surr

ENTRYPOINT ["surr-subscriptions"]
CMD ["run"]
