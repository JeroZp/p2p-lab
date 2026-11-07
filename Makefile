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
	docker compose -f infra/compose/cluster-5.yml up --build -d

cluster-down:
	docker compose -f infra/compose/cluster-5.yml down -v


PROJECT := p2p-lab
NET		:= $(PROJECT)_p2p-net
NODE    ?= 3
LATENCY ?= 200ms
FROM    ?= 1
TO      ?= 5

# Obeservation helpers

status:
	@for i in 1 2 3 4 5; do \
		v=$$(curl -s --max-time 1 localhost:910$$i/metrics | awk '/^p2p_connections_active/ {print $$2}'); \
		echo "node$$i connections: $${v:-DOWN}"; \
	done

transfer-check:
	@head -c 10M /dev/urandom > /tmp/chaos.bin
	@ROOT=$$(curl -s --max-time 10 --data-binary @/tmp/chaos.bin localhost:910$(FROM)/share); \
	echo "shared on node$(FROM), root: $$ROOT"; \
	start=$$(date +%s%N); \
	code=$$(curl -s --max-time 30 -o /tmp/chaos.out -w '%{http_code}' localhost:910$(TO)/fetch/$$ROOT); \
	end=$$(date +%s%N); \
	echo "fetch on node$(TO): HTTP $$code in $$(( (end - start) / 1000000 )) ms"; \
	if [ "$$code" = "200" ]; then sha256sum /tmp/chaos.bin /tmp/chaos.out; else echo "error: $$(cat /tmp/chaos.out)"; fi

# Failure injection

chaos-kill:
	docker kill $(PROJECT)-node$(NODE)-1

chaos-start:
	docker start $(PROJECT)-node$(NODE)-1

chaos-partition:
	docker network disconnect $(NET) $(PROJECT)-node$(NODE)-1

chaos- heal:
	docker network connect $(NET) $(PROJECT)-node$(NODE)-1

chaos-slow:
	docker run --rm --network container:$(PROJECT)-node$(NODE)-1 --cap-add NET_ADMIN \
		nicolaka/netshoot tc qdisc add dev eth0 root netem delay $(LATENCY)

chaos-fast:
	docker run --rm --network container:$(PROJECT)-node$(NODE)-1 --cap-add NET_ADMIN \
		nicolaka/netshoot tc qdisc del dev eth0 root