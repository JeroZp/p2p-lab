.PHONY: help run test lint build cluster-up cluster-down

# Default target 
help:
	@echo "p2p-lab — available targets:"
	@echo "  make run          - run a single node locally"
	@echo "  make test         - run all unit tests"
	@echo "  make lint         - run golangci-lint"
	@echo "  make build        - build the Go binary"
	@echo "  make cluster-up   - spin up local Docker cluster"
	@echo "  make cluster-down - tear down local Docker cluster"

run:
	cd go && go run ./cmd/node

test:
	cd go && go test ./...

lint:
	cd go && golangci-lint run ./...

build:
	cd go && go build -o ../bin/node ./cmd/node

cluster-up:
	docker compose -f infra/compose/cluster-5.yml up -d

cluster-down:
	docker compose -f infra/compose/cluster-5.yml down