ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
CONFIG_FILE := $(ROOT_DIR).golangci.yml

CACHE_DIR := $(ROOT_DIR).cache
BIN_DIR := $(ROOT_DIR)bin
VENV_DIR := $(ROOT_DIR).venv

# The nodes tests run against the node venv when it exists, so the Python side
# is exercised with the same interpreter (and the same GStreamer bindings) the
# node itself runs on. The venv lives at the repo root but the code lives in
# nodes/; create it with `uv sync`. Override with PYTHON=... to use another
# interpreter.
PYTHON ?= $(if $(wildcard $(VENV_DIR)/bin/python),$(VENV_DIR)/bin/python,python3)

# pyright reads the root pyproject.toml, which pins the venv it resolves imports
# against, so it has to run from the repo root with that venv's copy. Override
# with PYRIGHT=... to use another install.
PYRIGHT ?= $(if $(wildcard $(VENV_DIR)/bin/pyright),$(VENV_DIR)/bin/pyright,pyright)

export GOCACHE := $(CACHE_DIR)/go-build
export GOLANGCI_LINT_CACHE := $(CACHE_DIR)/golangci-lint

# Version stamped into every binary and logged at startup, so a running service
# can say which build it is. `git describe` gives the nearest tag, or the
# abbreviated commit when the tree has no tags yet, and appends -dirty when
# there are uncommitted changes. The `|| echo` covers a source archive with no
# .git directory. The node resolves the same string at startup (nodes/lib/
# version.py), so both sides of a mixed deployment report the same build.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)

# Build configuration variables
TAGS ?=
GOFLAGS ?=
LDFLAGS ?=
TESTFLAGS ?=
NODESTESTFLAGS ?=
TYPECHECKFLAGS ?=
RELEASE_TYPE ?= dev

# Shrink binary size when RELEASE_TYPE is set to "release"
ifeq ($(RELEASE_TYPE),release)
    LDFLAGS += -s -w
    GOFLAGS += -trimpath
endif

BUILD_TAGS_FLAG := $(if $(TAGS),-tags '$(TAGS)',)

# The version is always injected, so LDFLAGS=... (and the release -s -w) append
# to it rather than replacing it -- passing LDFLAGS on the command line used to
# be the only way to set linker flags, and it must not silently drop the build
# id. Every cmd/ package declares `var Version` in package main, which is what
# -X main.Version=... targets.
VERSION_LDFLAGS := -X main.Version=$(VERSION)
LDFLAGS_FLAG := -ldflags '$(strip $(LDFLAGS) $(VERSION_LDFLAGS))'

# Discover Go module paths, normalised so the module is addressable as `make .`
# rather than `make ./`. There is exactly one (the root): the services used to be
# separate modules with a go.work tying them together, which is what made them
# uninstallable with `go install ...@version` -- a module carrying a `replace`
# directive cannot be built from a version suffix. The find stays because it is
# what keeps this correct if a second module ever appears.
MODULES := $(patsubst ./%,%,$(shell find . -name "go.mod" -not -path '*/.*' -not -path '*/vendor/*' -exec dirname {} \;))

# Discover main package entrypoints under any cmd/ directory
CMD_DIRS := $(shell find . -type f -name "main.go" -path '*/cmd/*' -not -path '*/.*' -not -path '*/vendor/*' -exec dirname {} \;)

# Map directory names to build target rules (build-hub, build-rx, build-subscriptions, build-tx)
CMD_TARGETS := $(foreach dir,$(CMD_DIRS),build-$(notdir $(dir)))

# Map module paths to test target rules (test-root for the root module).
# TEST_DIRS adds one target per service directory, so `make test-hub` still means
# "test the hub's packages" now that the services are directories rather than
# modules of their own.
TEST_DIRS := tx rx hub subscriptions analytics
TEST_DIR_TARGETS := $(addprefix test-,$(TEST_DIRS))
TEST_TARGETS := $(addprefix test-,$(patsubst .,root,$(MODULES)))

.PHONY: all build lint test test-go typecheck test-root test-nodes prep-cache clean-cache clean $(MODULES) $(TEST_TARGETS) $(TEST_DIR_TARGETS) $(CMD_TARGETS)

# typecheck is in `all` but not in `lint`/`test`: it needs the node venv, while
# `make lint` and `make test-hub` should stay usable on a Go-only checkout.
all: lint test typecheck build

lint: prep-cache $(MODULES)

test: test-go test-nodes

# The Go half of `test`, split out because it has no Python dependency: CI runs
# it on a runner that has the cgo toolchain but no node venv.
test-go: prep-cache $(TEST_TARGETS)

prep-cache:
	@mkdir -p $(GOCACHE) $(GOLANGCI_LINT_CACHE) $(BIN_DIR)

$(MODULES):
	@echo "==> running golangci-lint: $@"
	@cd $@ && golangci-lint run --config=$(CONFIG_FILE)

# Test every package. The handler packages fail when a command a service
# advertises has no case in their table, so a new handler without a test breaks
# this. Pass TESTFLAGS=-race or TESTFLAGS=-count=1 as needed, and TAGS=va to
# also compile the vah264enc variant of tx.
$(TEST_TARGETS): test-%: prep-cache
	@echo "==> running go test: $(if $(filter root,$*),.,$*) (tags: '$(TAGS)')"
	@cd $(if $(filter root,$*),.,$*) && go test $(BUILD_TAGS_FLAG) $(TESTFLAGS) ./...

# One service's packages. `test-go` above covers everything; these are for the
# inner loop, where rebuilding the other four services' tests is pure latency.
$(TEST_DIR_TARGETS): test-%: prep-cache
	@echo "==> running go test: ./$*/... (tags: '$(TAGS)')"
	@go test $(BUILD_TAGS_FLAG) $(TESTFLAGS) ./$*/...

# Python tests for the nodes runtime (nodes/tests). Needs the node venv's
# dependencies -- aio_pika and numpy -- so it fails with a pointer rather than a
# bare ImportError when they are missing. Tests that need the GStreamer
# bindings skip themselves on a host without them. Pass NODESTESTFLAGS=-v for
# per-test output, and PYTHON=... to use another interpreter.
#
# `-t nodes` is load-bearing: it is what puts nodes/ on sys.path, so the tests'
# `from lib.core import ...` resolves without nodes/ being a package.
test-nodes:
	@echo "==> running python tests in nodes/ (interpreter: $(PYTHON))"
	@$(PYTHON) -c "import aio_pika, numpy" 2>/dev/null || { \
		echo "error: '$(PYTHON)' cannot import aio_pika and numpy"; \
		echo "       create the node venv from the repo root: uv sync"; \
		echo "       or run 'make test-nodes PYTHON=<interpreter>' with those packages installed"; \
		exit 1; \
	}
	@cd $(ROOT_DIR) && $(PYTHON) -m unittest discover -s nodes/tests -t nodes $(NODESTESTFLAGS)

build: prep-cache $(CMD_TARGETS)

# Static analysis for the Python side, configured in the root pyproject.toml
# (typeCheckingMode = "standard", venvPath = ".venv", extraPaths = ["nodes"]).
# It covers nodes/bustle.py, nodes/lib/ and nodes/tests/, and needs
# the gi stubs from PyGObject-stubs -- see pyproject.toml for why
# gst-python-stubs must not be installed.
# Pass TYPECHECKFLAGS=--outputjson for machine-readable output.
typecheck:
	@echo "==> type checking nodes/ (pyright: $(PYRIGHT))"
	@command -v $(PYRIGHT) >/dev/null 2>&1 || { \
		echo "error: pyright not found ('$(PYRIGHT)')"; \
		echo "       install it in the node venv: uv sync"; \
		echo "       or run 'make typecheck PYRIGHT=<path to pyright>'"; \
		exit 1; \
	}
	@cd $(ROOT_DIR) && $(PYRIGHT) $(TYPECHECKFLAGS)

# Pattern rule for individual binary builds
$(CMD_TARGETS): build-%: prep-cache
	$(eval CMD_PATH := $(shell find . -type d -path '*/cmd/$*' -not -path '*/.*' -not -path '*/vendor/*' | head -n 1))
	@if [ -z "$(CMD_PATH)" ]; then \
		echo "error: could not find command source directory for '$*'"; exit 1; \
	fi
	@echo "==> building '$*' [mode: $(RELEASE_TYPE)] from $(CMD_PATH) (tags: '$(TAGS)')..."
	@cd $(CMD_PATH) && go build $(BUILD_TAGS_FLAG) $(LDFLAGS_FLAG) $(GOFLAGS) -o $(BIN_DIR)/surr-$* .

clean-cache:
	@echo "==> clearing shared caches..."
	@rm -rf $(CACHE_DIR)

clean: clean-cache
	@echo "==> clearing output binary directory..."
	@rm -rf $(BIN_DIR)
