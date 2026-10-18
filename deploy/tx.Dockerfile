# The transmitter: pulls an RTSP source, transcodes with GStreamer, publishes
# SRT. The only image whose Go side is not pure Go -- go-gst/go-glib bind
# GStreamer through cgo, so GStreamer must be present at build AND run time.
#
# THREE stages, and the first one exists for a single plugin. tx needs
# `intersrc`, which comes from gst-plugins-rs (the Rust plugins) and from
# nowhere else: tx/internal/src/intersrc.go builds a bin around it and ghosts
# out its src pad, so with the element missing `setupDownstream` panics on a nil
# element. Debian packages no gst-plugins-rs at all -- `apt-cache policy
# gstreamer1.0-plugins-rs gst-plugins-rs` is empty on trixie -- so the only way
# to have it is to compile it. Hence `inter-build` below.
#
# The stage is worth understanding before changing it, because the tag is
# load-bearing. gst-plugins-rs is versioned in lockstep with GStreamer, and its
# `gstreamer-1.26.x` tags are the branch built for the 1.26 series: at
# gstreamer-1.26.11 the workspace declares `gst = { ... branch = "0.24" }`, i.e.
# the gstreamer-rs release that targets GStreamer 1.26, and `rust-version =
# "1.83"`, well under the toolchain the rust image ships. That matters twice
# over:
#
#   * Build time. gstreamer-rs asks pkg-config for gstreamer-1.0 at the version
#     of the series it targets, and trixie's GStreamer is 1.26.2, so a 1.26-series
#     tag resolves and a 1.28-series tag would not.
#   * Run time. A GStreamer plugin declares the GStreamer version it was built
#     against, and the loader refuses a plugin that wants a newer minor than the
#     runtime. A plugin built against 1.26 headers loads into trixie's 1.26
#     runtime; one built against 1.28 headers would be rejected outright.
#
# So: do not "update" GST_PLUGINS_RS_TAG to the newest tag. Move it with the
# distribution, and only to another tag of the same GStreamer series.
#
# The alternative considered and rejected was an Alpine base, because Alpine
# does package both gst-plugins-rs-rsinter and gst-vaapi prebuilt. It was
# rejected for the other four images' sake -- they gain nothing from Alpine, and
# the node image cannot use it at all because torch publishes no musl wheels --
# so the base split would have existed anyway, and a second distribution in the
# stack is a permanent cost paid to avoid one build stage.
#
# The dev packages are exactly the set that builds tx on a normal desktop.
# go-gst's generated cgo directives ask pkg-config for a long list (glib-2.0,
# gobject-2.0, gio-2.0, gstreamer-1.0 and roughly seventeen gstreamer-*-1.0
# modules including mpegts, play, player, gl, rtsp, sdp, pbutils, allocators,
# check and controller); those split across the three -dev packages installed in
# the second stage and nothing more.
#
# The runtime half matters as much as the build half: the pipeline is assembled
# from element NAMES at run time, so a missing plugin package is not a link
# error, it is a failure to create `srtsink` or `tsdemux` the first time a
# stream starts. The elements tx names are rtspsrc, srtsink, queue, h264parse,
# aacparse, rtph264depay, videoscale, videoconvert, vah264dec, openh264dec and
# mpegtsmux.
#
# tx also has two decoder backends behind build tags: h264dec_va.go (`//go:build
# va`) asks for `vah264dec`, and h264dec_default.go (`//go:build !va`, the
# default, so what `make build` produces) asks for `openh264dec`. openh264dec is
# installed here, from gstreamer1.0-plugins-bad. The VA plugin is installed too
# -- that same package ships it as libgstva.so -- but do not expect it to help
# inside a container: it registers its elements from the VA drivers it finds at
# plugin load, and with no driver present it registers none. `gst-inspect-1.0
# va` in this image reports `0 features:` and `vah264dec` is absent, which is
# why the default `!va` build is the one that runs here. A `-tags va` build
# additionally needs the host to pass /dev/dri and a VA driver through.
#
# This image uses the distribution's GStreamer rather than tx/dist/run.sh, which
# bundles private libraries and isolates GST_PLUGIN_PATH. That script is for
# shipping a self-contained binary to a host you do not control; inside a
# container the distribution packages are the smaller, updatable option.

FROM rust:1-slim-trixie AS inter-build

# Pinned to the GStreamer series the runtime stage ships. See the note above
# before moving it.
ARG GST_PLUGINS_RS_TAG=gstreamer-1.26.11

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache. The plugin links against gstreamer-base, hence the
# plugins-base dev package in addition to the core one.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      git \
      pkg-config \
      libgstreamer1.0-dev \
      libgstreamer-plugins-base1.0-dev

WORKDIR /src

# A shallow clone of the tag is all the plugin needs. `git` is also read at
# build time by gst-plugin-version-helper, which derives the plugin's reported
# version from the checkout -- which is why the tag (rather than a bare commit)
# is the right thing to ask for.
RUN git clone --depth 1 --branch "${GST_PLUGINS_RS_TAG}" \
      https://gitlab.freedesktop.org/gstreamer/gst-plugins-rs .

# -p builds the inter plugin and its dependencies, not the other ~40 plugins in
# the workspace. The registry cache mount is what keeps a rebuild after a
# Dockerfile edit from re-downloading the whole crate graph; the target mount is
# dropped afterwards on purpose, since the only artefact this stage owes the
# next one is the single .so.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p gst-plugin-inter \
 && install -D target/release/libgstrsinter.so /out/libgstrsinter.so

FROM golang:1.27-trixie AS build

# Injected at link time so the binary can name its own build. The same value is
# compiled into every service in this directory and is what `--version` and the
# startup log line report.
ARG VERSION=unknown

# Only the core dev package, and that is deliberate. What tx compiles is go-gst's
# `pkg/gst`, whose cgo directives ask for glib, gobject, gio and gstreamer-1.0
# plus the base/check/controller/net variants -- all of which libgstreamer1.0-dev
# provides (it pulls libglib2.0-dev). The one go-gst package that needs
# `gstreamer-mpegts-1.0` is gstmpegts, which tx does not import (check with
# `go list -deps ./...` in tx/). Adding libgstreamer-plugins-bad1.0-dev here used
# to be a no-op for the binary and dragged in libopencv-contrib-dev, libvtk9.3
# and libtk8.6 -- hundreds of megabytes of headers nothing compiles against.
# The runtime stage below still installs the plugins themselves; this stage only
# needs headers.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      pkg-config \
      libgstreamer1.0-dev

WORKDIR /src

# The build context is the repository root, not tx/, because tx/go.mod declares
# `replace github.com/booleanism/surr => ../` and so needs the root module
# beside it. Only the root module files and tx/ are copied -- the other three
# service modules are irrelevant to this binary.
COPY go.mod go.sum ./
COPY internal/ internal/
COPY tx/ tx/

WORKDIR /src/tx

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -trimpath \
      -ldflags "-s -w -X main.Version=${VERSION}" \
      -o /out/surr-tx ./cmd/tx

FROM debian:trixie-slim

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      gstreamer1.0-tools \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-plugins-ugly \
      gstreamer1.0-libav \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr \
 && install -d -o surr -g surr /data

# gstreamer1.0-tools is what puts `gst-inspect-1.0` in the image, which is how
# you answer "is the plugin actually in here?" -- `gst-inspect-1.0 intersrc` is
# the check that this whole first stage did its job.

COPY --from=inter-build /out/libgstrsinter.so /usr/local/lib/gstreamer-1.0/
COPY --from=build /out/surr-tx /usr/local/bin/surr-tx

# The plugin goes in its own directory rather than into the distribution's
# plugin path, so it is obvious which plugins this image added. GST_PLUGIN_PATH
# is additive: the directory is searched in addition to the system one, never
# instead of it, so every distribution plugin stays visible. (tx/dist/run.sh
# sets GST_PLUGIN_PATH and GST_PLUGIN_SYSTEM_PATH="" because it ships a private
# bundle; neither is needed or wanted here.)
ENV GST_PLUGIN_PATH=/usr/local/lib/gstreamer-1.0

USER surr

WORKDIR /data

ENTRYPOINT ["surr-tx"]
CMD ["--help"]
