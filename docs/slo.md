# Service Level Objectives

## Environment

5-node full-mesh cluster, Docker Compose on a single machine (WSL2).
Traffic generated with `scripts/slo-load.sh`: 200 gossip messages from
random nodes, 10 transfer rounds of 10MB (nodes 2-4 fetch first and
become seeders, then node5 fetches with 4 sources). Values read from the
SLOs dashboard over a 15-minute window.

## Results

| SLO | SLI | Target | Measured | Status |
|---|---|---|---|---|
| Gossip delivery | p95 of `p2p_gossip_delivery_seconds` | < 500ms | < 5ms (200 messages) | ✅ |
| DHT lookup | p99 of `p2p_dht_lookup_duration_seconds` | < 200ms | 5–10ms (~110 lookups) | ✅ |
| File transfer | fetches OK in < 5s / all fetches | ≥ 99% | 100% (40/40) | ✅ |
| 4-source transfer | p95 of `p2p_transfer_fetch_duration_seconds`, node5 | < 5s | 250–500ms | ✅ |
| Node recovery | seconds until all nodes back at 4 connections, after restart | < 10s | node1 5s, node3 4s, node5 0s | ✅ |

## Adaptations from the spec

- **4 sources, not 5.** On a 5-node cluster the downloading node can use
  at most the other 4. Real 5-source transfers need `cluster-10.yml`.
- **Gossip: delivery latency, not convergence.** Each node records
  broadcast-to-delivery time; the spec's "convergence" is the time until
  the *last* node receives it, so this p95 is slightly optimistic.
  Timestamps are comparable only because all containers share the host
  clock; across real machines, clock skew would break this.
- **Transfer time is measured inside `FetchFile`.** Timing `curl`
  included ~2.2s of HTTP delivery under latency (see chaos experiment 5).
- **The 99% target is ours.** The spec gives only the 5s threshold.
- **Recovery is measured per incident** with `make watch-status` during
  chaos tests. The dashboard's "min connections" panel shows recovery is
  in progress, not how long it took.

## Known limitations

- **Histogram resolution.** Values in a histogram's lowest bucket are
  interpolated: 4.756ms means "under 5ms". Read every value as a range
  between bucket boundaries. Add smaller buckets if precision matters.
- **Single machine.** No real network bandwidth or latency, so results
  are optimistic, and multi-source transfers show no speed-up here
  (per-peer bandwidth is not a bottleneck on one host).
- **Low event counts.** With 40 fetches, a single failure is 2.5% and
  would break a 99% SLO. Judging a 99% objective meaningfully needs
  hundreds of events: raise `ROUNDS` for a longer run.

## Reproduce

```
make cluster-down && make cluster-up && sleep 15
./scripts/slo-load.sh
# then open Grafana → Dashboards → p2p-lab → SLOs
```