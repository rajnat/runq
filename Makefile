GO ?= go
COMPOSE ?= docker-compose

.PHONY: help
help:
	@printf "Available targets:\n"
	@printf "  build         Build all Go packages\n"
	@printf "  migrate       Apply database migrations\n"
	@printf "  run-api       Run the API server\n"
	@printf "  run-scheduler Run the scheduler\n"
	@printf "  run-worker    Run the sample worker\n"
	@printf "  run-reaper    Run the reaper\n"
	@printf "  fmt           Format Go code\n"
	@printf "  test          Run the full test suite\n"
	@printf "  test-serial   Run the reliable serial test suite\n"
	@printf "  up            Start local Docker dependencies\n"
	@printf "  down          Stop local Docker dependencies\n"
	@printf "  demo          Run demo script\n"
	@printf "  e2e-smoke     Run end-to-end smoke script\n"
	@printf "  package-cli   Package the runq CLI release artifacts\n"

.PHONY: build
build:
	$(GO) build ./...

.PHONY: migrate
migrate:
	$(GO) run ./cmd/migrate

.PHONY: run-api
run-api:
	$(GO) run ./cmd/api-server

.PHONY: run-scheduler
run-scheduler:
	$(GO) run ./cmd/scheduler

.PHONY: run-worker
run-worker:
	$(GO) run ./cmd/worker

.PHONY: run-reaper
run-reaper:
	$(GO) run ./cmd/reaper

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: test
test:
	$(GO) test ./...

.PHONY: test-serial
test-serial:
	$(GO) test -p 1 ./...

.PHONY: up
up:
	$(COMPOSE) -f deploy/docker-compose.yml up -d

.PHONY: down
down:
	$(COMPOSE) -f deploy/docker-compose.yml down

.PHONY: demo
demo:
	./scripts/demo.sh

.PHONY: e2e-smoke
e2e-smoke:
	./scripts/e2e_smoke.sh

.PHONY: package-cli
package-cli:
	./scripts/package_runq_cli.sh $(VERSION)
