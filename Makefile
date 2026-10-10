ROOT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
CONFIG_FILE := $(ROOT_DIR).golangci.yml

CACHE_DIR := $(ROOT_DIR).cache
BIN_DIR := $(ROOT_DIR)bin

export GOCACHE := $(CACHE_DIR)/go-build
export GOLANGCI_LINT_CACHE := $(CACHE_DIR)/golangci-lint

# Build configuration variables
TAGS ?=
GOFLAGS ?=
LDFLAGS ?=
TESTFLAGS ?=
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

.PHONY: all build lint test test-root prep-cache clean-cache clean $(MODULES) $(TEST_TARGETS) $(CMD_TARGETS)

all: lint test build

lint: prep-cache $(MODULES)

test: prep-cache $(TEST_TARGETS)

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

build: prep-cache $(CMD_TARGETS)

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
