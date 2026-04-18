GO ?= go
GOLANGCI_LINT ?= golangci-lint

.PHONY: build test lint bench compliance

build:
	$(GO) build ./...

test:
	$(GO) test ./...

lint:
	$(GOLANGCI_LINT) run ./...

bench:
	$(GO) test -bench=. ./bench/...

compliance:
	./scripts/compliance.sh
