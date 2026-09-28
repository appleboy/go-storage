GO ?= go
TOOLS_MOD := -modfile=go.tools.mod

.PHONY: fmt
fmt: ## Format Go files using golangci-lint
	$(GO) tool $(TOOLS_MOD) golangci-lint fmt

.PHONY: lint
lint: ## Run golangci-lint
	$(GO) tool $(TOOLS_MOD) golangci-lint run --timeout=30m

.PHONY: install-tools fmt lint
install-tools: ## Download pinned Go tools
	$(GO) mod download $(TOOLS_MOD)
