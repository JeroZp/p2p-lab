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

**Observation / Result / Follow-up:** _TBD_

---

## Experiment 3 — Restart a killed node

**Hypothesis:** It rejoins the network.

**Observation / Result / Follow-up:** _TBD_

---

## Experiment 4 — Partition a node, then heal the network

**Hypothesis:** After healing, the node is not back in the P2P network.

**Observation / Result / Follow-up:** _TBD_

---

## Experiment 5 — 200ms latency on one node

**Hypothesis:** Only transfers get slower.

**Observation / Result / Follow-up:** _TBD_