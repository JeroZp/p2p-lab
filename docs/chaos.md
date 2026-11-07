# Chaos experiments

Each experiment follows the same structure: steady state, hypothesis
(written before running), method, observation, result, follow-up.

## Steady state

- `p2p_connections_active` = 4 on every node (full mesh of 5)
- DHT bootstrap finds 4 nodes
- `make transfer-check FROM=1 TO=5` returns HTTP 200, matching hashes
- 10MB fetch under 5 seconds (SLO)

Baseline: 10MB fetch across the Docker network in ~0.45s.

---

## Experiment 1 — Kill a node that doesn't hold the file

**Hypothesis:** Fetching still works. `p2p_connections_active` decreases
on the other nodes.

**Method:**
```
make chaos-kill NODE=3
make status
make transfer-check FROM=1 TO=5
```

**Observation (first run):** node3 went DOWN, but every surviving node
still reported 4 connections. Transfer node1 → node5 succeeded (519ms).

**Result:** Partially wrong. Transfers were unaffected, as predicted, but
the connection count did NOT decrease. Root cause: `serveConn` only
watched the node context, never its own connection, so it blocked forever
after the peer died. Dead peers stayed in the pool and goroutines leaked.
Writing a test for it exposed a second bug: `Shutdown()` raced with
`serveConn`'s cleanup, emptying the pool before closing sockets, so
sockets were never closed and peers were never notified.

**Follow-up:** Fixed. `serveConn` also exits on `conn.ctx.Done()` and
closes its own connection on exit; `removePeer` only removes the
connection if it is still the registered one. Rerun: survivors dropped
from 4 to 3 within 5s, transfer succeeded in 229ms. Peers received
FIN/RST from the killed process, so no half-open connections here.

---

## Experiment 2 — Kill the provider during a transfer

**Hypothesis:** The fetch stays incomplete without enough chunks,
with an error like "connection lost".

**Method:** 90MB file shared on node1, fetched on node5, node1 killed
~0.3s after the fetch started.

**Observation:** HTTP 502 after 10.4s: "chunk 22: chunk request to node1
timed out". Chunks 0-21 had already arrived and were kept by node5.

**Result:** Mostly right: the fetch failed and the file stayed incomplete.
But the error was a 10s timeout, not "connection lost". In-flight
requests only wait for reply / timeout / engine stop and never learn the
peer died, even though the core layer removed the peer within ms.
Later chunks failed instantly ("no connection to peer").

**Follow-up:** (1) fail pending requests when their peer disconnects;
(2) consider cancelling remaining fetches when no other provider exists;
(3) multiple providers per chunk, with retry on another provider.
Partial chunks are preserved (resumable), but stranded: node5 does not
announce them and node1 never rejoins (see experiment 3).

---

## Experiment 3 — Restart a killed node

**Hypothesis:** It rejoins the network.

**Observation:** Both restarted nodes kept their NodeID (key persisted
in the Docker volume). node3 reconnected only to node1 and node2 (its
`-dial` list): 2 connections; node4/node5 never redialed it. node1 has no
`-dial` list and came back with 0 connections, bootstrap found 0 nodes.
Sharing on node1 then fetching on node5 failed: HTTP 502, manifest not
found (13ms).

**Result:** Partially wrong. Identity survives restarts, but rejoining
depends on the static `-dial` list. node3 rejoined partially; node1 (the
bootstrap node) stayed isolated indefinitely. Recovery SLO (< 10s) not
met for node1.

**Follow-up:** Connectivity is established once at startup and never
maintained. Needed: a reconnect loop with backoff, and peer discovery so
nodes learn addresses beyond their static list. Also: a single bootstrap
node is a single point of failure for joining.

---

## Experiment 4 — Partition a node, then heal the network

**Hypothesis:** After healing, the node is not back in the P2P network.

**Method:** `docker network disconnect` node3, check connection counts
at 5s, 65s and ~185s (node3's own view via `docker exec`), then
`docker network connect` and check again.

**Observation:** At 5s and 65s every node, including the isolated node3,
still reported 4 connections. By ~185s survivors reported 3 and node3
reported 0. After healing, node3 stayed at 0 connections.

**Result:** Heal prediction correct: node3 did not rejoin, even though
it has a `-dial` list (dialing only happens at startup). New finding:
a partition produces half-open connections. No FIN/RST is sent, our code
has no liveness check, and detection relied on Go's default TCP
keepalive (~15s idle + 9 probes × 15s ≈ 150s). For ~2.5 minutes metrics
showed a healthy full mesh while node3 was unreachable.

**Follow-up:** Application-level heartbeat using the unused TypePing /
TypePong: ping every few seconds, close the connection after ~3 missed
intervals. Plus connection maintenance (same as experiment 3).
---

## Experiment 5 — 200ms latency on one node

**Hypothesis:** Only transfers get slower.

**Method:** 200ms egress delay on node3 via `tc netem` (netshoot sidecar).
Transfer-check node1 → node5 and node1 → node3, before and during.

**Observation:**
| | Baseline | 200ms on node3 |
|---|---|---|
| Share on node1 | 136 / 154 ms | 1936 / 1899 ms |
| Fetch on node5 | 274 ms | 220 ms |
| Fetch on node3 | 262 ms | 4760 ms |

**Result:** Wrong. Transfers not involving node3 were unaffected (node5
read DHT records from local replicas, chunks came from node1). DHT work
on a healthy node got ~14x slower: each FindNode round waits for all
alpha=3 replies, and node3 is in almost every round (tail latency
amplification). Fetching on node3 got ~18x slower because the delay also
applies to its TCP ACKs (bandwidth-delay product), landing at 4.76s,
just under the 5s SLO.

**Follow-up:** Lookups should not wait for the slowest peer: continue a
round once enough replies arrive, use shorter per-RPC timeouts, demote
slow peers.

## Summary

| # | Experiment | Prediction | Outcome |
|---|---|---|---|
| 1 | Kill non-provider node | Partially right | Found ghost peers + shutdown socket leak (fixed) |
| 2 | Kill provider mid-transfer | Mostly right | Fails after a 10s timeout, not instantly |
| 3 | Restart killed node | Partially wrong | Identity kept; rejoin is partial, bootstrap node isolated |
| 4 | Partition, then heal | Right on heal | ~150s of half-open connections, no rejoin |
| 5 | 200ms latency on one node | Wrong | Slows DHT on healthy nodes, not unrelated transfers |

### Prioritized follow-ups

1. ~~Heartbeats: detect dead peers in seconds (exp 4)~~ ✅ done
2. ~~Connection maintenance: reconnect with backoff (exp 3, 4)~~ ✅ done
3. Fail in-flight requests when their peer disconnects (exp 2)
4. Multiple providers per chunk, retry on another provider (exp 2)
5. Lookups that don't wait for the slowest peer (exp 5)

---

## Reruns after heartbeats and reconnection

Changes: connections exchange heartbeats every 2s and close after 6s of
silence; `Node.Maintain` redials lost peers with exponential backoff
(500ms → 5s cap, ±20% jitter).

| | Before | After |
|---|---|---|
| Restart node1 (exp 3) | 0 connections, forever | 4 connections at t+1s |
| Partition detection (exp 4) | ~150s (TCP keepalive) | 6–9s (heartbeat) |
| Rejoin after heal (exp 4) | never | ≤5s |
| Recovery SLO (< 10s) | not met | met |

**Tooling bug found during the rerun:** the first rerun showed node4/node5
never reconnecting to node3 after healing. The new failure log revealed
`lookup node3: no such host`: `docker network connect` reattached the
container without its Compose service alias, so the name `node3` no
longer resolved. The application was fine; the chaos "heal" step did not
restore the original state. Fixed with `--alias node$(NODE)`. Lesson:
verify that failure injection and recovery steps themselves are correct,
or the experiment produces false conclusions.