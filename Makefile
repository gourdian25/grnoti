# File: Makefile

.PHONY: help test race coverage coverage-summary coverage-check bench lint vet fmt clean deps precommit prerelease tag release goreleaser-check guard-version

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
