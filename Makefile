ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
CONFIG_FILE := $(ROOT_DIR).golangci.yml

CACHE_DIR := $(ROOT_DIR).cache
BIN_DIR := $(ROOT_DIR)bin
NODES_DIR := $(ROOT_DIR)nodes

# The nodes tests run against the node venv when it exists, so the Python side
# is exercised with the same interpreter (and the same GStreamer bindings) the
# node itself runs on. Override with PYTHON=... to use another interpreter.
PYTHON ?= $(if $(wildcard $(NODES_DIR)/.venv/bin/python),$(NODES_DIR)/.venv/bin/python,python3)

# pyright reads nodes/pyproject.toml, which pins the venv it resolves imports
# against, so it has to run from nodes/ with that venv's copy. Override with
# PYRIGHT=... to use another install.
PYRIGHT ?= $(if $(wildcard $(NODES_DIR)/.venv/bin/pyright),$(NODES_DIR)/.venv/bin/pyright,pyright)

export GOCACHE := $(CACHE_DIR)/go-build
export GOLANGCI_LINT_CACHE := $(CACHE_DIR)/golangci-lint

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
LDFLAGS_FLAG := $(if $(LDFLAGS),-ldflags '$(LDFLAGS)',)

# Discover Go module paths (root + submodules), normalised so a module is
# addressable as `make hub` / `make test-hub` rather than `make test-./hub`
MODULES := $(patsubst ./%,%,$(shell find . -name "go.mod" -not -path '*/.*' -not -path '*/vendor/*' -exec dirname {} \;))

# Discover main package entrypoints under any cmd/ directory
CMD_DIRS := $(shell find . -type f -name "main.go" -path '*/cmd/*' -not -path '*/.*' -not -path '*/vendor/*' -exec dirname {} \;)

# Map directory names to build target rules (build-hub, build-rx, build-subscriptions, build-tx)
CMD_TARGETS := $(foreach dir,$(CMD_DIRS),build-$(notdir $(dir)))

# Map module paths to test target rules (test-root, test-hub, test-rx, test-subscriptions, test-tx)
TEST_TARGETS := $(addprefix test-,$(patsubst .,root,$(MODULES)))

.PHONY: all build lint test typecheck test-root test-nodes prep-cache clean-cache clean $(MODULES) $(TEST_TARGETS) $(CMD_TARGETS)

# typecheck is in `all` but not in `lint`/`test`: it needs the node venv, while
# `make lint` and `make test-hub` should stay usable on a Go-only checkout.
all: lint test typecheck build

lint: prep-cache $(MODULES)

test: prep-cache $(TEST_TARGETS) test-nodes

prep-cache:
	@mkdir -p $(GOCACHE) $(GOLANGCI_LINT_CACHE) $(BIN_DIR)

$(MODULES):
	@echo "==> running golangci-lint in module: $@"
	@cd $@ && golangci-lint run --config=$(CONFIG_FILE)

# Test every module. The handler packages fail when a command a service
# advertises has no case in their table, so a new handler without a test breaks
# this. Pass TESTFLAGS=-race or TESTFLAGS=-count=1 as needed, and TAGS=va to
# also compile the vah264enc variant of tx.
$(TEST_TARGETS): test-%: prep-cache
	@echo "==> running go test in module: $(if $(filter root,$*),.,$*) (tags: '$(TAGS)')"
	@cd $(if $(filter root,$*),.,$*) && go test $(BUILD_TAGS_FLAG) $(TESTFLAGS) ./...

# Python tests for the nodes runtime (nodes/tests). Needs the node venv's
# dependencies -- aio_pika and numpy -- so it fails with a pointer rather than a
# bare ImportError when they are missing. Tests that need the GStreamer
# bindings skip themselves on a host without them. Pass NODESTESTFLAGS=-v for
# per-test output, and PYTHON=... to use another interpreter.
test-nodes:
	@echo "==> running python tests in nodes/ (interpreter: $(PYTHON))"
	@cd $(NODES_DIR) && $(PYTHON) -c "import aio_pika, numpy" 2>/dev/null || { \
		echo "error: '$(PYTHON)' cannot import aio_pika and numpy"; \
		echo "       create the node venv: python3 -m venv nodes/.venv && nodes/.venv/bin/pip install -r nodes/requirements.txt"; \
		echo "       or run 'make test-nodes PYTHON=<interpreter>' with those packages installed"; \
		exit 1; \
	}
	@cd $(NODES_DIR) && $(PYTHON) -m unittest discover -s tests -t . $(NODESTESTFLAGS)

build: prep-cache $(CMD_TARGETS)

# Static analysis for the Python side, configured in nodes/pyproject.toml
# (typeCheckingMode = "standard", venvPath = ".venv"). It covers bustle.py,
# core/, lib/ and tests/, and needs the gi stubs from PyGObject-stubs -- see
# nodes/requirements.txt for why gst-python-stubs must not be installed.
# Pass TYPECHECKFLAGS=--outputjson for machine-readable output.
typecheck:
	@echo "==> type checking nodes/ (pyright: $(PYRIGHT))"
	@command -v $(PYRIGHT) >/dev/null 2>&1 || { \
		echo "error: pyright not found ('$(PYRIGHT)')"; \
		echo "       install it in the node venv: nodes/.venv/bin/pip install pyright PyGObject-stubs"; \
		echo "       or run 'make typecheck PYRIGHT=<path to pyright>'"; \
		exit 1; \
	}
	@cd $(NODES_DIR) && $(PYRIGHT) $(TYPECHECKFLAGS)

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
