.PHONY: build test fmt check

VERSION ?= $(shell git describe --tags --always --dirty)
LDFLAGS := -X main.version=$(VERSION)

build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/herdr-bots ./cmd/herdr-bots

test:
	go test -race ./...

# fmt rewrites formatting; check only verifies it.
fmt:
	gofmt -l -w .

check:
	go mod verify
	@unformatted=$$(gofmt -l .); if [ -n "$$unformatted" ]; then \
		echo "gofmt needed (run: make fmt)"; echo "$$unformatted"; exit 1; \
	fi
	go vet ./...
	go test -race ./...
	go build -trimpath -ldflags "$(LDFLAGS)" ./...
	sh -n herdr-bots
	sh -n assays/release-gate.sh
	sh -n assays/launcher-assay.sh
	./assays/launcher-assay.sh
	./assays/release-gate.sh
