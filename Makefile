SHELL := /bin/bash
.DEFAULT_GOAL := help

MODULE    := github.com/marketplace
SERVICES  := auth catalog gateway inventory notification order payment
MODULES   := pkg $(addprefix services/,$(SERVICES))
GOBIN     ?= $(shell go env GOPATH 2>/dev/null)/bin
COMPOSE   := docker compose -f deploy/docker-compose.yml --env-file .env

# Build metadata injected into every binary.
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE      ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS   := -X $(MODULE)/pkg/version.Version=$(VERSION) \
             -X $(MODULE)/pkg/version.Commit=$(COMMIT) \
             -X $(MODULE)/pkg/version.Date=$(DATE)

# ---------------------------------------------------------------- meta

.PHONY: help
help: ## Show available targets
	@echo "marketplace — available targets:"
	@grep -hE '^[a-zA-Z0-9_%-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- tools

.PHONY: tools
tools: ## Install codegen / lint tooling into $(GOBIN)
	go install github.com/bufbuild/buf/cmd/buf@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "tools installed into $(GOBIN)"

# --------------------------------------------------------------- proto

.PHONY: proto
proto: ## Generate Go code from .proto definitions
	cd proto && PATH="$(GOBIN):$$PATH" buf generate
	@echo "generated code refreshed"

.PHONY: proto-lint
proto-lint: ## Lint .proto files and check for breaking changes
	cd proto && PATH="$(GOBIN):$$PATH" buf lint

# --------------------------------------------------------------- build

.PHONY: build
build: ## Build all service binaries into ./bin
	@mkdir -p bin
	@for s in $(SERVICES); do \
		echo "building $$s..."; \
		( cd services/$$s && go build -ldflags "$(LDFLAGS)" -o ../../bin/$$s ./cmd/$$s ) || exit 1; \
	done
	@echo "building devtoken..."
	@( cd services/gateway && go build -o ../../bin/devtoken ./cmd/devtoken )
	@echo "all services built into ./bin"

.PHONY: build-%
build-%: ## Build a single service, e.g. make build-order
	@mkdir -p bin
	cd services/$* && go build -ldflags "$(LDFLAGS)" -o ../../bin/$* ./cmd/$*

# ---------------------------------------------------------------- test

.PHONY: test
test: ## Run unit tests (no external infrastructure required)
	@$(MAKE) --no-print-directory for-each-module GOAL="go test ./... -count=1"

.PHONY: test-race
test-race: ## Run unit tests with the race detector
	@$(MAKE) --no-print-directory for-each-module GOAL="go test ./... -count=1 -race"

.PHONY: test-integration
test-integration: ## Run integration tests (embedded Postgres/Kafka/Redis, no Docker needed)
	@$(MAKE) --no-print-directory for-each-module GOAL="go test ./... -count=1 -tags=integration"

.PHONY: cover
cover: ## Produce an HTML coverage report
	@rm -f coverage.out
	@$(MAKE) --no-print-directory for-each-module GOAL="go test ./... -count=1 -covermode=atomic -coverprofile=coverage.$$(basename $$PWD).out"
	@echo "per-module coverage written; open a specific coverage.*.out with 'go tool cover'"

# The workspace root is not a Go module (go.work only), so `go test ./...` from
# the root fails. Every module-aware target runs the goal inside each module.
.PHONY: for-each-module
for-each-module:
	@for m in $(MODULES); do \
		printf '\033[36m==>\033[0m %s\n' "$$m"; \
		( cd $$m && $(GOAL) ) || exit 1; \
	done

# ---------------------------------------------------------------- lint

.PHONY: fmt
fmt: ## Format Go sources
	gofmt -s -w .

.PHONY: vet
vet: ## Run go vet
	@$(MAKE) --no-print-directory for-each-module GOAL="go vet ./..."

.PHONY: tidy
tidy: ## Tidy every module
	@for m in pkg services/*; do \
		if [ -f "$$m/go.mod" ]; then echo "tidy $$m"; ( cd $$m && go mod tidy ) || exit 1; fi; \
	done

# ------------------------------------------------------------ infra

.PHONY: up
.PHONY: env
env: ## Create .env from .env.example when it does not exist yet
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "created .env from .env.example — review it before deploying anywhere real"; \
	fi

.PHONY: up
up: env ## Start the whole stack in Docker
	$(COMPOSE) up -d --build
	@set -a; [ -f .env ] && . ./.env; set +a; \
	echo ""; \
	echo "stack is starting:"; \
	echo "  gateway    http://localhost:$${GATEWAY_HOST_PORT:-8080}"; \
	echo "  jaeger     http://localhost:$${JAEGER_UI_HOST_PORT:-16686}"; \
	echo "  grafana    http://localhost:$${GRAFANA_HOST_PORT:-3000}"; \
	echo "  prometheus http://localhost:$${PROMETHEUS_HOST_PORT:-9090}"; \
	echo ""; \
	echo "follow startup with: make logs"

.PHONY: down
down: ## Stop the stack and remove volumes
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show stack status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: logs-%
logs-%: ## Tail logs from one service, e.g. make logs-order
	$(COMPOSE) logs -f --tail=200 $*

.PHONY: k8s-apply
k8s-apply: ## Apply Kubernetes manifests
	kubectl apply -k deploy/k8s

.PHONY: k8s-delete
k8s-delete: ## Delete Kubernetes manifests
	kubectl delete -k deploy/k8s

# ---------------------------------------------------------------- e2e

.PHONY: e2e
e2e: ## Run end-to-end scenarios against a running stack
	go test ./test/e2e/... -count=1 -v -tags=e2e

.PHONY: k6
k6: ## Run the checkout load test with k6 (needs k6 on PATH)
	@set -a; [ -f .env ] && . ./.env; set +a; \
	BASE_URL="http://localhost:$${GATEWAY_HOST_PORT:-8080}" k6 run test/load/checkout.js

# ---------------------------------------------------------------- demos

.PHONY: token
token: ## Print a development access token, e.g. make token CUSTOMER=cust-1
	@cd services/gateway && go run ./cmd/devtoken -customer "$${CUSTOMER:-cust-demo}"

.PHONY: devtoken
devtoken: ## Build the development token helper into ./bin
	@mkdir -p bin
	@cd services/gateway && go build -o ../../bin/devtoken ./cmd/devtoken

.PHONY: demo
demo: devtoken ## Show the checkout saga rolling back, against a running stack
	@./scripts/demo.sh

# --------------------------------------------------------------- misc

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin dist coverage.out cover.html

.PHONY: rename
rename: ## Rename the Go module path, e.g. make rename MODULE=github.com/you/marketplace
	@test -n "$(NEW)" || { echo "usage: make rename NEW=github.com/you/marketplace"; exit 1; }
	@grep -rl '$(MODULE)' --include='*.go' --include='go.mod' --include='go.work' --include='*.proto' --include='*.yaml' --include='*.yml' . \
		| xargs sed -i '' 's|$(MODULE)|$(NEW)|g'
	@echo "module renamed to $(NEW) — run 'make tidy' next"
