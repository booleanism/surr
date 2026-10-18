# The data plane's SRT relay: terminates the publisher's SRT ingress and
# exposes the SRT egress that nodes pull frames from.
#
# The only image that needs libsrt. haivision/srtgo is pure cgo with
# `#cgo LDFLAGS: -lsrt` in a dozen files and does NOT vendor the library, so
# the system one is required at BOTH ends: the -dev package provides the
# linker name (libsrt.so) at build time, and the runtime package provides the
# SONAME (libsrt.so.1.5) the linked binary actually asks for. Debian ships no
# unversioned .so in the runtime package, which is the classic "works in the
# builder, libsrt.so.1.5 not found at run time" split.
#
# The openssl flavour is chosen to match what the Go binary links. It is
# co-installable with the gnutls one that gstreamer1.0-plugins-bad drags in
# (used by the tx and node images), because the two install under different
# names -- libsrt.so.1.5 vs libsrt-gnutls.so.1.5.

ARG GO_VERSION=1.27

FROM golang:${GO_VERSION}-trixie AS build

ARG VERSION=unknown

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends libsrt-openssl-dev

WORKDIR /src

COPY go.mod go.sum ./
COPY internal/ internal/
COPY rx/ rx/

WORKDIR /src/rx

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /out/surr-rx ./cmd/rx

FROM debian:trixie-slim

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libsrt1.5-openssl \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr

COPY --from=build /out/surr-rx /usr/local/bin/surr-rx

USER surr

WORKDIR /home/surr

# Both sockets are UDP. 4200 is where tx publishes (ingress); 4201 is what
# nodes subscribe to (egress). Neither is required to be published to the
# host when tx and the nodes also run in this compose stack -- publishing is
# for the case where only part of the system is containerised.
EXPOSE 4200/udp
EXPOSE 4201/udp

ENTRYPOINT ["surr-rx"]
CMD ["run"]
