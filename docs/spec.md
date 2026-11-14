# p2p-lab — Project Specification

**Version:** 1.0  
**Profile targets:** Software Engineer · DevOps / SRE  
**Primary language:** Go  
**Secondary language:** Python (informed rewrite)  
**Environment:** WSL2 (Kali Linux) + Docker + local cluster

---

## 1. Purpose

This project exists to build a deep, working understanding of peer-to-peer architectures by implementing one from scratch — not by using existing P2P libraries, but by constructing each layer manually and observing how it behaves.

The goal is twofold: understand the distributed systems theory behind real P2P systems (BitTorrent, IPFS, Kademlia, blockchain), and apply professional DevOps/SRE practices to a distributed system that fails in interesting, non-trivial ways.

By the end of the project you will have:

- A working multi-node P2P network running locally in Docker
- A DHT-based distributed key-value store
- A file chunking and transfer system with content addressing
- Full observability: metrics, traces, and dashboards
- A CI/CD pipeline that spins up a real P2P network in each test run
- A complete Python rewrite of the same system, done from understanding — not from tutorials

---

## 2. Repository Structure

```
p2p-lab/
│
├── go/                        # Primary implementation
│   ├── cmd/
│   │   └── node/              # Main entrypoint — starts a P2P node
│   ├── internal/
│   │   ├── common/            # Node identity, config, shared types
│   │   ├── core/              # Raw TCP, connection management, messaging
│   │   ├── gossip/            # Epidemic broadcast protocol
│   │   ├── dht/               # Kademlia DHT
│   │   └── transfer/          # File chunking, Merkle verification
│   └── go.mod
│
├── python/                    # Secondary implementation (rewrite)
│   ├── p2plab/
│   │   ├── common/
│   │   ├── core/
│   │   ├── gossip/
│   │   ├── dht/
│   │   └── transfer/
│   └── pyproject.toml
│
├── infra/
│   ├── docker/
│   │   ├── Dockerfile.go
│   │   └── Dockerfile.python
│   ├── compose/
│   │   ├── cluster-5.yml      # 5-node local network
│   │   └── cluster-10.yml     # 10-node local network
│   └── k8s/                   # Kubernetes manifests (stretch goal)
│       ├── deployment.yaml
│       └── service.yaml
│
├── observability/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── dashboards/
│   │       ├── network.json
│   │       ├── dht.json
│   │       └── transfer.json
│   └── otel/
│       └── collector.yaml
│
├── .github/
│   └── workflows/
│       ├── test.yml
│       ├── build.yml
│       └── integration.yml
│
├── docs/
│   ├── spec.md                # This document
│   ├── architecture.md
│   └── runbook.md
│
└── Makefile                   # Common tasks: run, test, lint, cluster-up
```

---

## 3. Scope

### In scope

- Multi-node TCP communication with no external P2P libraries
- Node identity based on cryptographic key pairs (Ed25519)
- Gossip-based message propagation (epidemic broadcast)
- Simplified Kademlia DHT: node IDs, XOR distance, routing tables, iterative lookups
- Distributed key-value storage across DHT nodes
- File chunking and reassembly with SHA-256 content addressing
- Merkle tree verification of file chunks
- Per-node Prometheus metrics endpoint
- OpenTelemetry distributed tracing across multi-hop operations
- Grafana dashboards for network topology, DHT state, and transfer metrics
- Docker Compose multi-node cluster for local development and testing
- GitHub Actions CI pipeline including integration tests on a real node cluster
- Python rewrite of all modules above

### Out of scope

**Production NAT traversal (STUN / TURN / ICE)**  
Real-world peers behind home and corporate routers cannot connect directly without negotiation. STUN discovers a peer's public IP; TURN relays traffic when direct connection fails; ICE negotiates which path to use. Implementing this correctly is protocol engineering work that teaches networking infrastructure, not P2P concepts. All nodes in this project run on the same Docker virtual network, which sidesteps NAT entirely. Understanding *why* NAT traversal is necessary is in scope; implementing it is not.

**Byzantine fault tolerance**  
The threat model for this project is nodes that crash, restart, or go offline — not nodes that actively lie, forge messages, or collude. BFT protocols (PBFT, Tendermint, HotStuff) are substantial research-grade systems that sit on top of everything built here. They are a natural next project, not part of this one.

**BitTorrent / IPFS wire-protocol compatibility**  
The DHT and file transfer modules are *inspired by* Kademlia and BitTorrent respectively, but are not byte-for-byte compatible with real clients. Achieving wire compatibility turns the project into a spec compliance exercise — parsing exact byte offsets, matching handshake sequences — without deepening understanding of the underlying algorithms. The goal is to understand why BitTorrent works, not to reimplement it.

**Public network deployment**  
All nodes run on `localhost` or within a controlled Docker network. Exposing nodes to the public internet introduces security exposure, potential legal concerns around file distribution, and infrastructure complexity (TLS certificates, firewall rules, DDoS surface) that are out of proportion with the project's learning goals. A controlled multi-VM cloud test is listed as a stretch goal.

---

## 4. Tech Stack

| Layer | Go (primary) | Python (rewrite) |
|---|---|---|
| Networking | `net` package, raw TCP | `asyncio`, raw sockets |
| Concurrency | goroutines + channels | `asyncio` coroutines |
| Serialization | `encoding/json`, `protobuf` | `msgpack`, `dataclasses` |
| Crypto | `crypto/ed25519`, `crypto/sha256` | `cryptography`, `hashlib` |
| Metrics | `prometheus/client_golang` | `prometheus_client` |
| Tracing | `go.opentelemetry.io/otel` | `opentelemetry-sdk` |
| Testing | `testing`, `testify` | `pytest`, `pytest-asyncio` |
| Container | Docker, Compose | same |
| CI | GitHub Actions | same |

---

## 5. Phases

Each phase builds directly on the previous one. Code is never discarded — it becomes a dependency of the next module.

---

### Phase 1 — Core: node identity and direct communication

**Goal:** Two nodes can find each other, establish a TCP connection, exchange structured messages, and cleanly handle disconnection.

**Concepts covered:**
- TCP lifecycle: `SYN`, `SYN-ACK`, `ACK`, `FIN`
- Why NAT makes direct P2P connections hard (observed, not solved)
- Node identity: why a P2P node needs a stable, unforgeable ID
- Message framing over a stream protocol: length-prefix encoding
- Connection state management: connecting, connected, disconnecting, dead

**Deliverables:**
- `NodeID` type: 256-bit identifier derived from an Ed25519 public key
- `Node` struct with a listener and connection pool
- Wire protocol: length-prefixed JSON messages with a type field
- `Dial(addr)` and `Accept()` methods
- Graceful shutdown with context cancellation
- Unit tests: connection establishment, message round-trip, clean disconnect

**Tools to run alongside:**
```bash
ss -tnp          # observe socket states
tcpdump -i lo    # watch your own packets
```

---

### Phase 2 — Gossip: epidemic message propagation

**Goal:** A message injected at one node propagates to all reachable nodes in the network without any central coordinator.

**Concepts covered:**
- Epidemic algorithms: SI (susceptible–infected) model
- Fan-out: how many peers to forward to
- Message deduplication: why you need seen-message tracking
- Convergence time: how long until all nodes receive a message
- The tradeoff between fan-out and network load

**Deliverables:**
- `GossipMessage` type with ID, originator, TTL, and payload
- `Gossip(msg)` method: forward to K random peers not in the message's path
- Seen-message cache with TTL expiry (prevents infinite loops)
- Metrics: messages forwarded, duplicates dropped, propagation latency
- Integration test: inject one message into a 10-node cluster, verify all nodes receive it
- Experiment: vary fan-out K from 1 to 5, plot convergence time vs load

**Real systems using this:** Bitcoin transaction broadcast, Ethereum's devp2p, Cassandra anti-entropy.

---

### Phase 3 — DHT: distributed hash table (Kademlia)

**Goal:** Any node can store a key-value pair and any other node can retrieve it, without anyone knowing the full network topology.

**Concepts covered:**
- XOR distance metric: why it produces a consistent and balanced topology
- Routing table: k-buckets organized by distance prefix
- Iterative lookup: find the K closest nodes to a key in O(log n) hops
- Replication: store values at the K closest nodes for fault tolerance
- Churn: what happens to stored values when nodes join and leave

**Deliverables:**
- `NodeID.XORDistance(other NodeID)` method
- `RoutingTable` with k-bucket management and bucket splitting
- `FindNode(target NodeID)` RPC: return K closest known nodes
- `Store(key, value)` and `FindValue(key)` RPCs
- Iterative lookup algorithm
- Metrics: lookup hop count, routing table size per bucket, store/retrieve latency
- Integration test: store 100 keys across a 10-node cluster, retrieve all from random nodes

**Real systems using this:** BitTorrent (mainline DHT), IPFS (libp2p Kademlia), Ethereum node discovery.

---

### Phase 4 — Transfer: file chunking and content addressing

**Goal:** A file can be split across multiple nodes and reassembled by any peer, with cryptographic proof that chunks have not been altered.

**Concepts covered:**
- Content addressing: identifying data by its hash, not its location
- Chunking strategy: fixed-size vs variable-size (Rabin fingerprint)
- Merkle trees: how a single root hash verifies an arbitrary number of chunks
- Parallel chunk fetching: why BitTorrent downloads from multiple peers simultaneously
- Rarest-first strategy: why nodes prioritize chunks that fewer peers have

**Deliverables:**
- `ChunkFile(path, chunkSize)` → list of `Chunk{Index, Hash, Data}`
- `MerkleTree` builder and proof verifier
- Chunk announcement via DHT: store `chunkHash → peerAddr` mappings
- `FetchFile(rootHash)`: discover chunk locations via DHT, fetch in parallel, verify and reassemble
- Metrics: chunk fetch latency per peer, verification failures, parallel fetch throughput
- Integration test: distribute a 10MB file across 5 nodes, fetch from a sixth node that has none of it

**Real systems using this:** BitTorrent, IPFS, Git (content-addressed objects), Dat protocol.

---

### Phase 5 — Stretch: mini blockchain (optional)

**Goal:** Use the gossip and DHT layers to build a minimal append-only ledger with proof-of-work consensus.

**Note:** This phase is explicitly optional. It is included because it connects everything built in Phases 1–4 to blockchain concepts without requiring a new architecture. It is not required for the project to be complete or portfolio-ready.

**Concepts covered:**
- Block structure: hash pointer chaining
- Proof-of-work: adjustable difficulty, nonce search
- Chain selection: longest valid chain rule
- Transaction propagation via gossip
- Block storage and retrieval via DHT

---

## 6. DevOps / SRE Layer

This layer runs in parallel with all phases — it is not a separate phase. Observability is added to each module as it is built, not retrofitted at the end.

### Metrics (Prometheus)

Every node exposes `GET /metrics` on a configurable port. Metrics are organized by module:

| Metric | Type | Description |
|---|---|---|
| `p2p_connections_active` | Gauge | Current open TCP connections |
| `p2p_messages_sent_total` | Counter | Messages sent, labelled by type |
| `p2p_messages_received_total` | Counter | Messages received, labelled by type |
| `p2p_gossip_forwarded_total` | Counter | Gossip messages forwarded |
| `p2p_gossip_duplicates_total` | Counter | Deduplicated gossip messages |
| `p2p_dht_lookup_duration_seconds` | Histogram | End-to-end DHT lookup latency |
| `p2p_dht_lookup_hops` | Histogram | Hop count per DHT lookup |
| `p2p_dht_routing_table_size` | Gauge | Routing table entries per node |
| `p2p_transfer_chunk_duration_seconds` | Histogram | Per-chunk fetch latency |
| `p2p_transfer_bytes_total` | Counter | Total bytes transferred |

### Tracing (OpenTelemetry)

Trace context is propagated in message headers across nodes. Key spans:

- `gossip.propagate` — full propagation lifetime of one message, with peer hops as child spans
- `dht.lookup` — iterative lookup with one child span per RPC hop
- `transfer.fetch_file` — full file fetch, with one child span per chunk

Traces are exported to an OpenTelemetry Collector → Grafana Tempo.

### Dashboards (Grafana)

Three dashboards:

**Network overview** — active nodes, connection count per node, message rate across the cluster, gossip propagation heatmap.

**DHT state** — routing table sizes, lookup latency distribution, store/retrieve ratios, nodes with the most stored keys.

**Transfer** — in-progress transfers, chunk throughput per peer, verification failure rate, file reassembly time.

### Local cluster (Docker Compose)

```bash
make cluster-up N=10    # spin up 10 nodes + Prometheus + Grafana + Tempo
make cluster-down       # tear everything down
make chaos-kill N=3     # kill 3 random nodes, observe recovery
```

Each node container:
- Runs the Go binary with a unique port and node ID
- Has its `/metrics` endpoint scraped by Prometheus
- Exports traces to the OTel collector
- Is connected via a Docker bridge network (`p2p-net`)

### CI/CD (GitHub Actions)

Three workflows:

**`test.yml`** — runs on every push. Unit tests for all modules. Linting with `golangci-lint`.

**`build.yml`** — builds Docker images for both Go and Python implementations, pushes to GitHub Container Registry on merge to main.

**`integration.yml`** — runs on pull requests. Spins up a 5-node Docker Compose cluster, runs integration tests against the live network (gossip propagation, DHT store/retrieve, file transfer), then tears down. This is a real distributed system test in CI.

### SLOs

Practice defining and measuring SLOs against your own system:

| SLO | Target | Measurement |
|---|---|---|
| Gossip convergence (10 nodes) | p95 < 500ms | Trace: `gossip.propagate` duration |
| DHT lookup latency (10 nodes) | p99 < 200ms | Metric: `p2p_dht_lookup_duration_seconds` |
| File transfer (10MB, 5 sources) | < 5 seconds | Metric: `p2p_transfer_bytes_total` rate |
| Node recovery after crash | < 10 seconds | Manual chaos test |

### Chaos engineering

After the cluster is stable, deliberately break it and measure recovery:

```bash
make chaos-kill N=2         # kill 2 nodes mid-transfer
make chaos-partition A=1,2 B=3,4,5   # split the network
make chaos-slow NODE=3 LATENCY=200ms  # add latency to one node
```

Observe how DHT routing degrades, how gossip re-routes, and how in-progress transfers behave. This is the most instructive SRE exercise in the project.

---

## 7. Python Rewrite

The Python rewrite begins after the Go implementation is complete and tested. By that point, you know exactly what each module does — the rewrite is a language evaluation, not a rediscovery.

**Rewrite order:** `core` → `gossip` → `dht` → `transfer`

**What to pay attention to per module:**

- `core`: asyncio's event loop vs Go's goroutine scheduler. How does backpressure work differently?
- `gossip`: Python's GIL and what it means for true parallelism. asyncio tasks vs goroutines.
- `dht`: Go interfaces vs Python duck typing. How does the lack of static types affect refactoring?
- `transfer`: `asyncio.gather()` for parallel chunk fetching vs Go's `sync.WaitGroup` + channels.

**The rewrite is complete when:** the Python cluster can interoperate with the Go cluster — a Go node and a Python node exchange gossip messages and cooperate on DHT lookups. This validates that the wire protocol and algorithms are correct and language-agnostic.

---

## 8. Testing Strategy

| Test type | Tool | When it runs |
|---|---|---|
| Unit tests | `go test` / `pytest` | Every push (CI) |
| Integration tests | Custom harness + Docker Compose | Every PR (CI) |
| Chaos tests | `make chaos-*` | Manual, after each phase |
| Performance baseline | `go test -bench` | Manual, before and after optimizations |
| Cross-language interop | Integration harness | After Python rewrite |

Integration test harness starts a real cluster, injects operations, and asserts outcomes — no mocking of network behavior. If the test passes, the distributed system actually works.

---

## 9. Milestones

| Milestone | Description |
|---|---|
| M1 — Foundation | Repo structure, Makefile, Docker base images, CI skeleton |
| M2 — Phase 1 complete | Two nodes connect and exchange messages; unit tests pass |
| M3 — Phase 2 complete | 10-node cluster with gossip; Prometheus metrics live; Grafana network dashboard |
| M4 — Phase 3 complete | DHT store/retrieve across 10 nodes; tracing live; DHT dashboard |
| M5 — Phase 4 complete | File transfer end-to-end; all dashboards live; chaos tests documented |
| M6 — SRE complete | SLOs defined and measured; CI integration tests passing; runbook written |
| M7 — Python rewrite | All modules ported; Go/Python interop test passing |
| M8 — Stretch (optional) | Mini blockchain on top of gossip + DHT |

---

## 10. Stretch Goals

These are explicitly not in the main track. They are listed so you know what comes next if the project ends and you want to keep going.

- **Kubernetes deployment:** deploy the cluster to Minikube, then to a managed K8s cluster (GKE free tier). Practice writing `Deployment`, `Service`, and `ConfigMap` manifests. Add a `HorizontalPodAutoscaler` and observe what happens when nodes scale.
- **Multi-machine test:** run nodes on two or three cloud VMs (e.g. fly.io or Railway free tier) with real network hops between them. This is where NAT traversal becomes a real problem.
- **Go → Rust port:** after Python, port the core and DHT modules to Rust using `tokio`. Understand why `rust-libp2p` exists.
- **gRPC wire protocol:** replace the JSON wire protocol with Protocol Buffers over gRPC. Measure the performance difference.

---

## 11. References

These are the primary sources to study alongside each phase — not tutorials, but the original papers and specifications that the implementations are based on.

- **Kademlia:** Maymounkov & Mazières, *Kademlia: A Peer-to-peer Information System Based on the XOR Metric* (2002)
- **Epidemic algorithms:** Demers et al., *Epidemic Algorithms for Replicated Database Maintenance* (1987)
- **BitTorrent:** Bram Cohen, *Incentives Build Robustness in BitTorrent* (2003)
- **Merkle trees:** Ralph Merkle, *A Digital Signature Based on a Conventional Encryption Function* (1987)
- **Go networking:** *The Go Programming Language*, Donovan & Kernighan — chapters 8 (goroutines) and 9 (concurrency)
- **SRE practice:** *Site Reliability Engineering*, Beyer et al. (Google) — free at sre.google/books

---

## 12. Implementation notes

Sections 1–11 are the original plan, kept as written. This section records
what was actually built and every place the implementation deviated from
the plan, and why.

### Final status

| Milestone | Status |
|---|---|
| M1 — Foundation | ✅ Done |
| M2 — Phase 1: identity + TCP | ✅ Done |
| M3 — Phase 2: gossip + metrics + network dashboard | ✅ Done |
| M4 — Phase 3: DHT + tracing + DHT dashboard | ✅ Done |
| M5 — Phase 4: transfer + dashboards + chaos experiments | ✅ Done |
| M6 — SLOs, runbook, CI integration tests | ✅ Done (`v0.6.0`) |
| M7 — Python rewrite, Go/Python interop | ✅ Done for core, gossip and DHT (`v0.7.0`) |
| M8 — Mini blockchain | ⏭ Not done (optional) |

### Decisions by area

**Toolchain**
- Go 1.25: Prometheus and OpenTelemetry client versions required it. The
  Docker build image follows `go.mod`.
- Python 3.11+, flat package (`python/p2plab/*.py`) instead of one
  subpackage per module, which is more natural in Python.

**Wire protocol**
- Length-prefixed JSON, as planned. No protobuf/gRPC.
- The format turned out to be defined implicitly by Go's `encoding/json`
  (NodeIDs as integer arrays, byte slices as base64, capitalized keys on
  DHT entries). It is now written down in [protocol.md](protocol.md) and
  pinned by golden fixtures generated from the Go code (`go/cmd/golden`).

**Core**
- Message routing by type (`Node.Subscribe`): gossip and DHT originally
  competed for one `Inbound` channel and stole each other's messages.
- Handshake: the dialing side goes first; a guard ignores repeated
  handshakes (an early version looped forever and inflated the
  connection gauge).
- Liveness: a dedicated `heartbeat` message every 2s, connection closed
  after 6s of silence. Added after a partition went undetected for
  ~150s (TCP keepalive). `ping`/`pong` stay ordinary message types.
- Connection maintenance: `Node.Maintain` redials lost peers with
  exponential backoff (500ms → 5s cap, ±20% jitter).
- Topology: full mesh where each pair has exactly one designated dialer
  (the higher-numbered node dials the lower ones), so duplicate
  connections cannot occur and no tie-break rule is needed.

**Gossip**
- As planned (fan-out 3, TTL 5, deduplication by message ID).
- `gossip.propagate` spans were replaced by a simpler measurement: the
  originator's send time travels in the payload, and each node records
  delivery latency in `p2p_gossip_delivery_seconds`. Valid because all
  containers share one clock.

**DHT**
- K = 8, alpha = 3. Lookup rounds wait at most 50ms after the first reply,
  so one slow peer no longer slows every lookup (found in chaos
  experiment 5).
- Records hold a set of up to 20 values, so a chunk can have several
  providers. No record expiry or republishing.
- Peers enter the routing table the moment they connect
  (`Node.OnPeerConnected`). Polling left a window where a node was
  connected but could not replicate; the first CI run found it.
- Not implemented: bucket splitting and ping-before-evict.

**Transfer**
- Fixed 1MB chunks, SHA-256 addressing, manifest verified against the
  Merkle root. DHT keys: `manifest:<root>` and `chunk:<hash>`.
- Multiple providers with failover; a node that downloads a file
  announces itself as a provider (seeding).
- In-flight requests fail immediately when the peer disconnects
  (`Node.PeerDone`) instead of waiting for the timeout.
- Not implemented: variable-size (Rabin) chunking, rarest-first.

**API** (not in the original plan; every node exposes it)
- `POST /share`, `GET /fetch/{root}`, `POST /gossip`,
  `POST/GET /dht/{key}`, `GET /metrics`.

**Observability**
- Metrics added beyond the plan's table: `p2p_gossip_delivery_seconds`,
  `p2p_transfer_fetch_duration_seconds`, `p2p_transfer_fetches_total`,
  `p2p_transfer_verification_failures_total`. One Prometheus registry per
  instance, so tests can create many nodes.
- Tracing: OTel collector → Tempo, spans `dht.lookup`/`dht.rpc` and
  `transfer.fetch_file`/`transfer.fetch_chunk`. Trace context is **not**
  propagated across nodes, so each node's work is a separate trace.
- A fourth Grafana dashboard: SLOs.

**SRE**
- SLOs measured and documented in [slo.md](slo.md), with adaptations:
  4 sources instead of 5 (a 5-node cluster has at most 4 other peers),
  delivery latency instead of convergence, and a 99% target chosen by us.
- Chaos targets inject faults into a specific node rather than random
  ones, so experiments are reproducible. Results in [chaos.md](chaos.md);
  operational playbooks in [runbook.md](runbook.md).

**CI**
- One workflow (`integration.yml`) with two jobs instead of three
  workflows: `go vet` + unit tests, then a real 5-node cluster and
  `scripts/integration-test.sh`. The integration suite enforces the
  10-second recovery SLO.
- Not implemented: `build.yml` (image push to GHCR), `golangci-lint` in CI.

**Python**
- Ported: identity, wire format, core node, gossip, DHT. A Python node
  joins the Go cluster, exchanges gossip and cooperates on DHT lookups.
- Simplifications: routing table as a dict sorted by XOR distance (no
  k-buckets); no transfer module; no tracing.
- Interop was verified manually; the Python node is not part of the
  Docker cluster or CI.

**Not done**
- `cluster-10.yml`, Kubernetes, multi-machine deployment, gRPC, the Rust
  port and M8. For Kubernetes, the planned approach is a StatefulSet with
  a headless Service rather than a Deployment: nodes need stable names and
  one persistent volume each, so their NodeIDs survive restarts.

---

*This document is a living spec. Update it as the project evolves — decisions made during implementation should be recorded here, not just in commit messages.*