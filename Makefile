.PHONY: run-api run-controller migrate-up migrate-down migrate-version migrate-force db-bootstrap test test-unit test-integration test-e2e

MIGRATE ?= migrate
MIGRATIONS_PATH ?= migrations
DB_URL ?= postgres://postgres:postgres@localhost:5432/stream_orchestrator?sslmode=disable
TEST_DB_URL ?= $(DB_URL)
DB_ADMIN_URL ?= postgres://localhost:5432/postgres?sslmode=disable
APP_DB_USER ?= postgres
APP_DB_PASSWORD ?= postgres

run-api:
	go run ./cmd/orchestrator-api

run-controller:
	go run ./cmd/orchestrator-controller

migrate-up:
	$(MIGRATE) -path $(MIGRATIONS_PATH) -database "$(DB_URL)" up

migrate-down:
	$(MIGRATE) -path $(MIGRATIONS_PATH) -database "$(DB_URL)" down 1

migrate-version:
	$(MIGRATE) -path $(MIGRATIONS_PATH) -database "$(DB_URL)" version

migrate-force:
	@test -n "$(VERSION)" || (echo "VERSION is required. Example: make migrate-force VERSION=1" && exit 1)
	$(MIGRATE) -path $(MIGRATIONS_PATH) -database "$(DB_URL)" force $(VERSION)

db-bootstrap:
	DB_URL="$(DB_URL)" DB_ADMIN_URL="$(DB_ADMIN_URL)" APP_DB_USER="$(APP_DB_USER)" APP_DB_PASSWORD="$(APP_DB_PASSWORD)" MIGRATE_BIN="$(MIGRATE)" MIGRATIONS_PATH="$(MIGRATIONS_PATH)" ./scripts/bootstrap-db.sh

test:
	go test ./... -count=1

test-unit:
	go test ./internal/... -count=1 -v

test-integration:
	@test -n "$(TEST_DB_URL)" || (echo "TEST_DB_URL is required" && exit 1)
	@echo "WARNING: integration tests may TRUNCATE tables in TEST_DB_URL=$(TEST_DB_URL)"
	TEST_DB_URL="$(TEST_DB_URL)" go test ./test/integration -count=1 -v

test-e2e:
	@test -n "$(TEST_DB_URL)" || (echo "TEST_DB_URL is required" && exit 1)
	@test -n "$(TEST_RABBITMQ_URL)" || (echo "TEST_RABBITMQ_URL is required" && exit 1)
	@echo "WARNING: e2e tests may TRUNCATE tables in TEST_DB_URL=$(TEST_DB_URL)"
	TEST_DB_URL="$(TEST_DB_URL)" TEST_RABBITMQ_URL="$(TEST_RABBITMQ_URL)" go test ./test/e2e -count=1 -v
