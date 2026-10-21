# File: Makefile

.PHONY: help test race coverage coverage-summary coverage-check bench lint vet fmt clean deps docker-up docker-create docker-down precommit prerelease tag release goreleaser-check guard-version

GO := go
MODULE := github.com/gourdian25/grnoti
COVERAGE_MIN := 95
VERSION ?=

# The shared Postgres/Redis/Mongo/Kafka containers every gourdian25 repo
# tests against. grsentry's docker-compose.yml is the source of truth for
# how they're built (images, ports, credentials, volumes); this path is
# where it sits in the usual side-by-side checkout.
GRSENTRY_COMPOSE ?= ../grsentry/docker-compose.yml
SHARED_CONTAINERS := gourdian-postgres gourdian-redis gourdian-mongo-auth gourdian-kafka

# wait_for runs the command $(2) once a second until it succeeds, giving up
# after 60 tries with an error naming container $(1), never looping forever.
wait_for = for i in $$(seq 60); do { $(2); } >/dev/null 2>&1 && exit 0; sleep 1; done; \
	echo "FAIL: $(1) is not ready after 60s; see 'docker logs $(1)'"; exit 1

help:
	@echo "Makefile targets for grnoti:"
	@echo ""
	@echo "  make test             Run all tests"
	@echo "  make race             Run tests with race detector (mandatory before any commit touching experiment.go, workerpool.go, dlq.*.go, or any store)"
	@echo "  make coverage         Generate HTML coverage report"
	@echo "  make coverage-summary Show coverage summary by function"
	@echo "  make coverage-check   Check the package meets the $(COVERAGE_MIN)% threshold"
	@echo "  make bench            Run benchmarks"
	@echo "  make lint             Run linters (requires golangci-lint)"
	@echo "  make vet              Run go vet"
	@echo "  make fmt              Format code"
	@echo "  make clean            Clean build artifacts"
	@echo "  make deps             Verify and tidy dependencies"
	@echo "  make docker-up        Start the shared Postgres/Redis/Mongo/Kafka test containers (idempotent)"
	@echo "  make docker-down      Stop those containers (state preserved for a fast restart)"
	@echo "  make precommit        fmt + vet + lint + race + coverage-check — run before every commit"
	@echo "  make prerelease       precommit + goreleaser-check — run before tagging a release"
	@echo "  make tag VERSION=vX.Y.Z         Create and push a git tag"
	@echo "  make release VERSION=vX.Y.Z     Tag, push, and run goreleaser release --clean"
	@echo "  make goreleaser-check           Dry run: validate config + snapshot release (no tag/push)"
	@echo ""
	@echo "grnoti is a single flat package (no subpackages) — every backend"
	@echo "(Mongo/Postgres/Redis/Kafka/FCM) lives in this one module. Backend"
	@echo "tests need real local services: run make docker-up (see CLAUDE.md)."
	@echo "Scope a run to one backend while iterating, e.g.:"
	@echo "  go test -run TestDeviceStore_Contract/Memory ./..."

test:
	@echo "Running tests..."
	$(GO) test -count=1 -timeout=5m -cover ./...
	@echo "Tests passed"

race:
	@echo "Running tests with race detector..."
	$(GO) test -race -timeout 5m ./...
	@echo "Race detector tests passed"

coverage:
	@echo "Generating coverage report..."
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "HTML coverage report saved as coverage.html"

coverage-summary:
	@echo "Coverage summary by function:"
	@$(GO) test -coverprofile=coverage.out ./...
	@$(GO) tool cover -func=coverage.out

# grnoti is one flat package, so coverage is a single overall number, not a
# per-backend gate. Scoped to `.` so the untested sqlc-generated
# internal/postgresdb and the example program don't dilute it (see
# docs/architecture.md §5). Use `make coverage-summary` for a per-function
# breakdown when a specific new file needs closer attention.
coverage-check:
	@echo "Checking package meets $(COVERAGE_MIN)% coverage..."
	@out=$$($(GO) test -cover . 2>&1); \
	pct=$$(echo "$$out" | grep -o '[0-9.]*%' | tr -d '%'); \
	if [ -z "$$pct" ]; then echo "FAIL: no coverage output"; exit 1; fi; \
	below=$$(awk -v p="$$pct" -v m="$(COVERAGE_MIN)" 'BEGIN { print (p < m) ? 1 : 0 }'); \
	if [ "$$below" = "1" ]; then \
		echo "FAIL: $$pct% is below $(COVERAGE_MIN)% threshold"; exit 1; \
	else \
		echo "OK: $$pct%"; \
	fi

bench:
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem -benchtime=10s ./...
	@echo "Benchmarks complete"

lint:
	@echo "Running linters..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run ./...
	@echo "Linting passed"

vet:
	@echo "Running go vet..."
	$(GO) vet ./...
	@echo "Vet analysis complete"

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	@echo "Code formatted"

clean:
	@echo "Cleaning build artifacts..."
	rm -f coverage.out coverage.html
	$(GO) clean ./...
	@echo "Clean complete"

deps:
	@echo "Verifying dependencies..."
	$(GO) mod verify
	@echo "Tidying dependencies..."
	$(GO) mod tidy
	@echo "Dependency verification complete"

# docker-up is idempotent and shares its containers with every other
# gourdian25 repo (each gets its own database/keyspace inside them; grnoti's
# is the grnoti_test database, created here). Containers that already exist,
# whoever made them, are just started. Missing ones are created from
# grsentry's docker-compose.yml when that checkout sits beside this one, so
# they're exactly what grsentry runs and a later `docker compose up` there
# finds its own containers; otherwise docker-create builds them by hand.
# Every wait below assumes that compose layout (mongod on 27018 inside its
# container, Confluent Kafka) and gives up after 60s rather than hanging.
docker-up:
	@missing=""; for c in $(SHARED_CONTAINERS); do docker inspect $$c >/dev/null 2>&1 || missing="$$missing $$c"; done; \
	if [ -z "$$missing" ]; then \
		echo "Starting the shared test containers..."; docker start $(SHARED_CONTAINERS) >/dev/null; \
	elif [ -f "$(GRSENTRY_COMPOSE)" ]; then \
		echo "Creating$$missing from $(GRSENTRY_COMPOSE)..."; docker compose -f "$(GRSENTRY_COMPOSE)" up -d; \
	else \
		echo "$(GRSENTRY_COMPOSE) not found; creating$$missing with docker run..."; $(MAKE) --no-print-directory docker-create; \
	fi
	@echo "Waiting for Postgres..."; $(call wait_for,gourdian-postgres,docker exec gourdian-postgres pg_isready -U postgres_user -d postgres)
	@docker exec gourdian-postgres psql -U postgres_user -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = 'grnoti_test'" | grep -q 1 || \
		docker exec gourdian-postgres createdb -U postgres_user grnoti_test
	@echo "Waiting for Redis..."; $(call wait_for,gourdian-redis,docker exec gourdian-redis redis-cli -a redis_password ping | grep -q PONG)
	@echo "Waiting for Mongo..."; $(call wait_for,gourdian-mongo-auth,docker exec gourdian-mongo-auth mongosh --port 27018 --quiet --eval 'db.runCommand({ping:1})')
	@docker exec gourdian-mongo-auth mongosh --port 27018 --quiet -u root -p mongo_password --authenticationDatabase admin --eval \
		'try { rs.status() } catch (e) { try { rs.initiate({_id: "rs0", members: [{_id: 0, host: "127.0.0.1:27018"}]}) } catch (e2) {} }' >/dev/null
	@echo "Waiting for the Mongo replica set..."; $(call wait_for,gourdian-mongo-auth,docker exec gourdian-mongo-auth mongosh --port 27018 --quiet -u root -p mongo_password --authenticationDatabase admin --eval 'rs.status().members[0].stateStr' | grep -q PRIMARY)
	@echo "Waiting for Kafka..."; $(call wait_for,gourdian-kafka,docker exec gourdian-kafka kafka-broker-api-versions --bootstrap-server localhost:9092)
	@echo "Docker test infrastructure ready (postgres/redis/mongo-auth/kafka, database grnoti_test)"

# docker-create builds whichever shared container is missing with plain
# docker run, for a checkout without grsentry beside it. Every image, port,
# credential and volume mirrors $(GRSENTRY_COMPOSE): keep the two in sync.
# The Mongo replica set is initiated by docker-up, as compose's one-shot
# mongo-init service does there.
docker-create:
	@docker inspect gourdian-postgres >/dev/null 2>&1 || docker run -d --name gourdian-postgres --restart unless-stopped -p 5432:5432 \
		-e POSTGRES_USER=postgres_user -e POSTGRES_PASSWORD=postgres_password -e POSTGRES_DB=grnoti_test \
		-v gourdian-postgres-data:/var/lib/postgresql postgres:18-alpine
	@docker inspect gourdian-redis >/dev/null 2>&1 || docker run -d --name gourdian-redis --restart unless-stopped -p 6379:6379 \
		-v gourdian-redis-data:/data redis:8-alpine redis-server --requirepass redis_password
	@docker inspect gourdian-mongo-auth >/dev/null 2>&1 || { \
		docker run --rm -v gourdian-mongo-keyfile:/keyfile-dir --entrypoint bash mongo:8.3 -c \
			'[ -f /keyfile-dir/mongo-keyfile ] || { openssl rand -base64 756 > /keyfile-dir/mongo-keyfile && chmod 400 /keyfile-dir/mongo-keyfile && chown 999:999 /keyfile-dir/mongo-keyfile; }' && \
		docker run -d --name gourdian-mongo-auth --restart unless-stopped -p 27018:27018 \
			-e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=mongo_password \
			-v gourdian-mongo-keyfile:/etc/mongo-keyfile-dir -v gourdian-mongo-data:/data/db \
			mongo:8.3 --port 27018 --replSet rs0 --keyFile /etc/mongo-keyfile-dir/mongo-keyfile; }
	@docker inspect gourdian-kafka >/dev/null 2>&1 || docker run -d --name gourdian-kafka --restart unless-stopped -p 9092:9092 \
		-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk -v gourdian-kafka-data:/var/lib/kafka/data confluentinc/cp-kafka:8.3.2

docker-down:
	@docker stop gourdian-postgres gourdian-redis gourdian-mongo-auth gourdian-kafka 2>/dev/null || true
	@echo "Stopped (containers preserved for a fast restart via 'make docker-up')"

# precommit is the standard local gate before every commit: format, then
# every static/dynamic check a CI run would also do, in cheapest-first
# order so a fast failure (fmt/vet/lint) doesn't wait on the slow ones
# (race, which spins up real Mongo/Postgres/Redis/Kafka backend tests).
precommit: fmt vet lint race coverage-check
	@echo "precommit checks passed"

# prerelease adds goreleaser's own config/build validation on top of
# precommit — run this before `make tag`/`make release`, not as a
# replacement for precommit during normal development.
prerelease: precommit goreleaser-check
	@echo "prerelease checks passed"

guard-version:
	@if [ -z "$(VERSION)" ]; then \
		echo "VERSION is required (example: make release VERSION=v0.1.0)"; \
		exit 1; \
	fi

tag: guard-version
	@echo "Tagging $(VERSION)..."
	git tag $(VERSION)
	git push origin $(VERSION)
	@echo "Tagged and pushed $(VERSION)"

release: guard-version tag
	@echo "Releasing $(VERSION) with goreleaser..."
	@which goreleaser > /dev/null || (echo "goreleaser not found. Install with: go install github.com/goreleaser/goreleaser/v2@latest" && exit 1)
	goreleaser release --clean
	@echo "Released $(VERSION)"

# Dry run: validates .goreleaser.yaml and builds a snapshot release locally
# without requiring a git tag or pushing anything.
goreleaser-check:
	@which goreleaser > /dev/null || (echo "goreleaser not found. Install with: go install github.com/goreleaser/goreleaser/v2@latest" && exit 1)
	goreleaser check
	goreleaser release --snapshot --clean

.DEFAULT_GOAL := help
