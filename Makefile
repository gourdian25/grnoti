# File: Makefile

.PHONY: help test race coverage coverage-summary coverage-check bench lint vet fmt clean deps docker-up docker-down precommit prerelease tag release goreleaser-check guard-version

GO := go
MODULE := github.com/gourdian25/grnoti
COVERAGE_MIN := 90
VERSION ?=

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
	@echo "tests need real local services; see CLAUDE.md for docker run commands."
	@echo "Scope a run to one backend while iterating, e.g.:"
	@echo "  go test -run TestTokenStore_Contract/Memory ./..."

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
# per-backend gate the way grcache/graudit's subpackage-per-backend layout
# allows — see docs/architecture.md for the tradeoff this accepts. Use
# `make coverage-summary` for a per-function breakdown when a specific new
# file needs closer attention.
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

# docker-up is idempotent: safe to run repeatedly, and safe to run
# alongside gourdiantoken/grcache/graudit's own `make docker-up` since
# every gourdian25 repo shares these same container names/ports (see
# CLAUDE.md) — each just gets its own database/keyspace inside them.
# Kafka and the Postgres/Redis auth needed here are grnoti-specific (no
# other repo needs Kafka); Mongo is provisioned but grnoti's own store
# code doesn't speak auth yet (tracked separately, see CLAUDE.md).
docker-up:
	@echo "Starting shared test containers..."
	@docker inspect gourdian-postgres >/dev/null 2>&1 || docker run -d --name gourdian-postgres -p 5432:5432 \
		-e POSTGRES_USER=postgres_user -e POSTGRES_PASSWORD=postgres_password -e POSTGRES_DB=grnoti_test postgres:16
	@docker start gourdian-postgres >/dev/null 2>&1 || true
	@docker inspect gourdian-redis >/dev/null 2>&1 || docker run -d --name gourdian-redis -p 6379:6379 redis:7 --requirepass redis_password
	@docker start gourdian-redis >/dev/null 2>&1 || true
	@docker volume create gourdian-mongo-keyfile >/dev/null
	@docker inspect gourdian-mongo-auth >/dev/null 2>&1 || (docker run --rm -v gourdian-mongo-keyfile:/keyfile-dir mongo:7 bash -c "openssl rand -base64 756 > /keyfile-dir/mongo-keyfile && chmod 400 /keyfile-dir/mongo-keyfile && chown 999:999 /keyfile-dir/mongo-keyfile" && docker run -d --name gourdian-mongo-auth -p 27018:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=mongo_password -v gourdian-mongo-keyfile:/etc/mongo-keyfile-dir mongo:7 --replSet rs0 --keyFile /etc/mongo-keyfile-dir/mongo-keyfile)
	@docker start gourdian-mongo-auth >/dev/null 2>&1 || true
	@docker inspect gourdian-kafka >/dev/null 2>&1 || docker run -d --name gourdian-kafka -p 9092:9092 apache/kafka:3.7.0
	@docker start gourdian-kafka >/dev/null 2>&1 || true
	@echo "Waiting for Postgres..."
	@until docker exec gourdian-postgres pg_isready -U postgres_user >/dev/null 2>&1; do sleep 1; done
	@docker exec gourdian-postgres psql -U postgres_user -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'grnoti_test'" | grep -q 1 || \
		docker exec gourdian-postgres psql -U postgres_user -d postgres -c "CREATE DATABASE grnoti_test"
	@echo "Waiting for Redis..."
	@until docker exec gourdian-redis redis-cli -a redis_password ping 2>/dev/null | grep -q PONG; do sleep 1; done
	@echo "Waiting for Mongo (auth + replica set)..."
	@until docker exec gourdian-mongo-auth mongosh --quiet -u root -p mongo_password --authenticationDatabase admin --eval 'db.runCommand({ping:1})' >/dev/null 2>&1; do sleep 1; done
	@docker exec gourdian-mongo-auth mongosh --quiet -u root -p mongo_password --authenticationDatabase admin --eval 'rs.initiate()' >/dev/null 2>&1 || true
	@echo "Waiting for Kafka..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do docker exec gourdian-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 2; done
	@echo "Docker test infrastructure ready (postgres/redis/mongo-auth/kafka)"

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
