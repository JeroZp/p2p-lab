# p2p-lab

![CI](https://github.com/JeroZp/p2p-lab/actions/workflows/integration.yml/badge.svg)

A peer-to-peer network built from scratch in Go (raw TCP, gossip, a
Kademlia-style DHT and content-addressed file transfer, no P2P libraries)
and operated like a production service: metrics, distributed tracing,
SLOs, chaos experiments, a runbook, and integration tests that run a real
5-node cluster in CI on every pull request.

## What it does

- **Identity:** Ed25519 key pair per node; NodeID = SHA-256(public key),
  persisted across restarts.
- **Core:** TCP with length-prefixed JSON framing, heartbeats (dead peers
  detected in 6–9s), automatic reconnection with exponential backoff and
  jitter, per-type message routing.
- **Gossip:** epidemic broadcast with fan-out 3, TTL and deduplication.
- **DHT:** XOR distance, k-buckets, iterative lookups that don't wait for
  slow peers, records that hold multiple values.
- **Transfer:** 1MB chunks addressed by SHA-256, manifests verified
  against a Merkle root, parallel fetching, multiple providers with
  failover, and seeding: every node that downloads a file can serve it.

## Architecture

```
          HTTP API:  POST /share   GET /fetch/{root}   POST /gossip   /metrics
                                        │
      ┌────────────────┬────────────────┴────────────────┬────────────────┐
      │     gossip     │              dht                │    transfer    │
      │   broadcast    │   routing table, store, lookups │ chunks, Merkle │
      │                │   (directory for transfer)      │ fetch, seeding │
      └───────┬────────┴────────────────┬────────────────┴───────┬────────┘
              └──────────────── message router ──────────────────┘
                                        │
             core: identity · TCP framing · heartbeats · reconnection
```

Observability: every node exposes Prometheus metrics and exports
OpenTelemetry traces (collector → Tempo). Grafana is provisioned with
four dashboards: network, DHT, transfer and SLOs.

## Quick start

Requirements: Docker with Compose v2, `make`, and Go 1.25+ for unit tests.
Developed on Windows 11 with WSL2.

```bash
make cluster-up          # 5 nodes + Prometheus, Grafana, Tempo, OTel collector

# share a file on node1; the response is its Merkle root
ROOT=$(curl -s --data-binary @myfile.bin http://localhost:9101/share)

# fetch it on node5
curl -s http://localhost:9105/fetch/$ROOT -o copy.bin

# broadcast a gossip message from node1
curl -X POST --data "hello" http://localhost:9101/gossip

make cluster-down
```

Grafana: http://localhost:3000 (admin / admin), folder **p2p-lab**.

## Results

Service level objectives, measured on the local 5-node cluster
(details and caveats in [docs/slo.md](docs/slo.md)):

| SLO | Target | Measured |
|---|---|---|
| Gossip delivery, p95 | < 500ms | < 5ms |
| DHT lookup, p99 | < 200ms | 5–10ms |
| Fetches OK in < 5s | ≥ 99% | 100% |
| 10MB fetch from 4 sources, p95 | < 5s | 250–500ms |
| Recovery after a node crash | < 10s | met (see slo.md) |

Chaos experiments, before and after the fixes they led to
([docs/chaos.md](docs/chaos.md)):

| Failure | Before | After |
|---|---|---|
| Network partition detected | ~150s (TCP keepalive) | 6–9s (heartbeats) |
| Partitioned node rejoins after healing | never | ≤ 5s |
| Bootstrap node restarted | isolated forever | reconnected in ~1s |
| Provider killed mid-transfer | fails after 10.4s timeout | fails in 0.37s |
| Original provider dead, file seeded elsewhere | HTTP 502 | HTTP 200 |
| Sharing next to a node with 200ms latency | ~1.9s | 0.25–0.49s |

## Engineering highlights

Bugs found by *running* the system under failure, not by reading it:

- **Ghost peers and a socket leak.** Killing a node left it in every
  peer's connection pool; the test for it exposed a shutdown race that
  never closed sockets. Neither showed up in unit tests.
- **Half-open connections.** A partition looked like a healthy mesh for
  ~2.5 minutes, until kernel TCP keepalive gave up. Fixed with
  application-level heartbeats.
- **Tail latency amplification.** One slow node made DHT lookups on
  healthy nodes ~14x slower, because each lookup round waited for every
  reply.
- **A silent fallback.** A config filename typo made the OTel collector
  run its default config; traces went nowhere for a whole phase. The
  collector now fails at startup instead.
- **A bug in the chaos tooling itself.** "Healing" a partition dropped
  the container's DNS alias, so the node looked permanently unreachable.
- **A race found by CI's first run.** Sharing a file one second after
  startup replicated it nowhere: routing tables were filled by polling.
  Now peers enter the routing table the moment they connect.

## Testing

| Level | Command | Runs |
|---|---|---|
| Unit | `make test` | on every PR |
| Integration (real 5-node cluster) | `make integration` | on every PR |
| Chaos | `make chaos-kill`, `chaos-partition`, `chaos-slow` | manually |

The integration suite checks mesh formation, gossip delivery, a 10MB
transfer, crash recovery within the 10s SLO, and that a file survives the
loss of its original provider.

## Documentation

- [docs/spec.md](docs/spec.md): project specification
- [docs/chaos.md](docs/chaos.md): chaos experiments, hypotheses and results
- [docs/slo.md](docs/slo.md): SLO definitions and measurements
- [docs/runbook.md](docs/runbook.md): symptom-based operational playbooks

## License

MIT