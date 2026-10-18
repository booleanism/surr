# A Python AI node (the object counter, `bustle`). This image carries the
# heaviest dependency set in the project: GStreamer via PyGObject, plus
# torch/libreyolo for inference.
#
# Debian trixie, the same base as every other image in this directory. The node
# is also the reason trixie is the right default rather than a preference:
# PyGObject 3.58 requires GLib >= 2.80 and looks up `girepository-2.0` through
# pkg-config, while bookworm's GLib is 2.74, so the build fails there outright.
# trixie ships GLib 2.84 and girepository-2.0 natively.
#
# PyGObject has no wheels -- it is built from source by meson against the
# system GLib -- which is why the -dev packages below are unavoidable rather
# than a shortcut.
#
# The venv lives at /opt/venv instead of the project's default ./.venv for two
# reasons: `COPY nodes/ nodes/` stays a clean copy of the source tree with no
# interpreter mixed into it, and the path is identical in both stages, which
# matters because the compiled PyGObject extension and the console scripts
# carry absolute interpreter paths baked in at build time.

FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim AS build

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache. PyGObject has no wheels, so this stage really does
# compile against these headers on every cold build.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      pkg-config \
      libglib2.0-dev \
      libgirepository-2.0-dev \
      libcairo2-dev

WORKDIR /app

# Only the manifest and the lock, so the dependency layer is cached across
# source edits. --frozen is the point: if uv.lock disagrees with
# pyproject.toml the build stops instead of silently resolving something
# else. The lock is also what pins torch to the CPU wheel index
# ([[tool.uv.index]] pytorch-cpu in pyproject.toml), so a non-frozen sync
# would happily pull the CUDA build.
COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

# Same build id the Go services are compiled with, so a mixed deployment can
# be diagnosed from the logs of either side.
ARG VERSION=unknown

# apt against BuildKit cache mounts: the .debs and the package index survive a
# rebuild instead of being downloaded again, and the `rm -rf /var/lib/apt/lists/*`
# that used to sit here is gone because the mounts never reach the image, so it
# would only wipe the cache.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      libglib2.0-0 \
      libgirepository-2.0-0 \
      libgstreamer1.0-0 \
      libgstreamer-plugins-base1.0-0 \
      gstreamer1.0-plugins-base \
      gstreamer1.0-plugins-good \
      gstreamer1.0-plugins-bad \
      gstreamer1.0-libav \
      gstreamer1.0-tools \
      gir1.2-glib-2.0 \
      gir1.2-gstreamer-1.0 \
      gir1.2-gst-plugins-base-1.0 \
 && useradd --uid 10001 --user-group --create-home --shell /usr/sbin/nologin surr

# The node's pipeline is built from element names, so these are load-bearing:
# srtsrc and tsdemux and h264parse come from plugins-bad, avdec_h264 from
# libav, and the gir1.2-* typelibs are what `gi.require_version('Gst', '1.0')`
# resolves against. Without them the import fails at startup rather than the
# pipeline failing later -- but they are runtime packages, not build ones.
#
# ca-certificates is not optional either: libreyolo downloads the detector
# weights over HTTPS the first time a node starts.

ENV VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    SURR_VERSION=${VERSION}

COPY --from=build /opt/venv /opt/venv

WORKDIR /app

# ./weights is where a bare model filename resolves to, and it is writable
# because the first run downloads into it. It is also the volume the compose
# file mounts, so the download survives a container rebuild.
RUN install -d -o surr -g surr /app/weights

COPY nodes/ nodes/

USER surr

ENTRYPOINT ["python", "nodes/bustle.py"]
CMD ["--help"]
