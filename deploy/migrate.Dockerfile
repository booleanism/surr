# The migration runner for the two SQLite databases (hub, subscriptions).
#
# This is a hand-built image because the official `migrate/migrate` one cannot
# talk to SQLite at all. golang-migrate gates every database driver behind a
# build tag, and its own Dockerfile builds with
#
#     DATABASE ?= postgres mysql redshift cassandra spanner cockroachdb \
#                 yugabytedb clickhouse mongodb sqlserver firebird neo4j pgx pgx5 rqlite
#
# -- sqlite3 appears only in DATABASE_TEST, never in DATABASE. The driver's
# registration file (internal/cli/build_sqlite3.go) carries `//go:build
# sqlite3`, so in the published image the driver is simply not linked in and
# every run ends with:
#
#     error: failed to open database: database driver: unknown driver sqlite3
#     (forgotten import?)
#
# The fix is the two things this file does that upstream does not: enable CGO
# and pass -tags sqlite3. mattn/go-sqlite3 is the same driver the hub and
# subscriptions binaries link (their own `sql.Open("sqlite3", ...)`), so the
# migration and the services agree on the file format and on what a "database
# is locked" error looks like.
#
# The alternative -- the pure-Go `sqlite` driver, which needs no CGO -- is not
# used because it is a different engine: migrations would then be applied by
# one SQLite implementation and read by another. The error message the services
# print (`migrate -database sqlite3://<db file> -path ... up`, see
# internal/db/schema.go) also names the sqlite3 scheme.

# Debian trixie throughout this directory: one distribution release answers
# "which libc, which OpenSSL, which GStreamer" for the whole stack, so a fix in
# one image is a fix in all of them. The one thing trixie costs is the Rust
# stage in tx.Dockerfile -- trixie packages no gst-plugins-rs, so tx builds the
# `intersrc` plugin from source.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS build

# Pinned: the runner decides what the schema looks like, so a floating version
# would let a rebuild change the database out from under a running service.
ARG MIGRATE_VERSION=v4.20.1

RUN CGO_ENABLED=1 go install -tags sqlite3 \
      github.com/golang-migrate/migrate/v4/cmd/migrate@${MIGRATE_VERSION}

FROM debian:trixie-slim

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr \
 && install -d -o surr -g surr /data

COPY --from=build /go/bin/migrate /usr/local/bin/migrate

# The UID matches every service image in this directory on purpose. Docker
# seeds a fresh named volume from the image's directory *including its
# ownership*, so running the migration as `surr` is what makes /data writable
# by the service that mounts the same volume afterwards.
USER surr

WORKDIR /data

ENTRYPOINT ["migrate"]
CMD ["-help"]
