GO ?= go
COMPOSE ?= docker-compose

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
