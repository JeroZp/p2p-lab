# p2p-lab runbook

Operational guide for the local cluster. Start from the **symptom** you
see, follow the playbook, and always finish with the **verify** step.
For background, see `chaos.md` (failure behaviour) and `slo.md` (targets).

## Quick reference

| What | Where |
|---|---|
| Grafana (dashboards) | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |
| Tempo (traces) | http://localhost:3200 |
| Node N metrics + API | http://localhost:910N |
| Node N P2P port | localhost:700N |

| Command | Use |
|---|---|
| `make cluster-up` / `make cluster-down` | Start / stop everything (down also wipes node identities) |
| `make status` | Connection count per node, `DOWN` if unreachable |
| `make watch-status SECONDS=N` | Connection counts every second |
| `make transfer-check FROM=a TO=b` | End-to-end health check: share on a, fetch on b |
| `docker logs p2p-lab-nodeN-1` | Logs of node N |

**Healthy state:** every node shows `4` connections, `transfer-check`
returns HTTP 200 with matching hashes, the SLOs dashboard is all green.

---

## Playbooks

### 1. A node shows `DOWN`

**Means:** the node's HTTP port doesn't answer. The process crashed, the
container stopped, or the container lost its network (see playbook 2).

**Diagnose:**
```
docker compose -f infra/compose/cluster-5.yml ps
docker logs p2p-lab-nodeN-1 2>&1 | tail -30
```
If the container is `Exited`, the logs show why it stopped.

**Mitigate:**
```
docker start p2p-lab-nodeN-1
```
The node keeps its identity (key in its Docker volume). Peers redial it
automatically with backoff (cap 5s).

**Verify:** `make watch-status SECONDS=10`: all nodes back at 4 within
~10s (recovery SLO).

---

### 2. Nodes are up but some have fewer than 4 connections

**Means:** part of the mesh is missing. After a crash or network blip
this is normal for a few seconds: dead connections are detected within
6–9s (heartbeat) and peers reconnect with backoff.

**Diagnose:** if it lasts more than ~15s, find out why reconnection fails:
```
docker logs p2p-lab-nodeN-1 2>&1 | grep -E "reconnect|connected to" | tail -10
```
- `no such host`: DNS can't resolve the node's name. A container that
  was reconnected to the network **without its Compose alias** is
  invisible to its peers. (Happened during chaos experiment 4.)
- `connection refused`: the target is up but not listening; check its logs.
- `i/o timeout` or `no route to host`: network problem between containers.

**Mitigate:**
- DNS / network: reattach with the alias:
  ```
  docker network disconnect p2p-lab_p2p-net p2p-lab-nodeN-1
  docker network connect --alias nodeN p2p-lab_p2p-net p2p-lab-nodeN-1
  ```
- Otherwise restart the affected node: `docker restart p2p-lab-nodeN-1`.

**Verify:** `make watch-status SECONDS=15`: all nodes at 4.

**Note:** if *every* node reconnected at the same time, the host itself
likely paused (WSL or laptop sleep). Heartbeats time out, then
everything heals on its own. No action needed.

---

### 3. Fetch fails with `manifest not found`

**Means:** the DHT has no record of the file. Usually the sharing node
was isolated when it shared (no peers to replicate to), or the file was
never shared in this cluster (`cluster-down` wipes everything).

**Diagnose:**
```
make status
docker logs p2p-lab-nodeN-1 2>&1 | grep "DHT bootstrap"
```
A sharing node with 0 connections, or `bootstrap: 0 peers`, explains it.

**Mitigate:** restore its connections (playbooks 1–2), then share the
file again.

**Verify:** `make transfer-check FROM=N TO=M`.

---

### 4. Fetch fails with `peer ... disconnected` or `no reachable provider`

**Means:** every node holding the chunks is unreachable. Files survive
the loss of a provider only if another node fetched them first
(seeding).

**Diagnose:** which nodes are down? `make status`.

**Mitigate:** bring a provider back (playbook 1), or share the file
again from any node that has it.

**Verify:** the same fetch returns HTTP 200.

---

### 5. Transfers or DHT lookups are slow (SLO panels red)

**Means:** usually **one** slow node. A single slow peer slows DHT work
on healthy nodes too (chaos experiment 5), and transfers to or from it.

**Diagnose:** find the slowest node. In Grafana → Explore → Prometheus:
```
histogram_quantile(0.95, sum by (instance, le) (increase(p2p_transfer_chunk_duration_seconds_bucket[15m])))
```
Then check that node for injected latency:
```
docker run --rm --network container:p2p-lab-nodeN-1 nicolaka/netshoot tc qdisc show dev eth0
```
`netem delay ...` in the output means latency was injected.

**Mitigate:** `make chaos-fast NODE=N` to remove injected latency; if
there is none, restart the node.

**Verify:** run `make transfer-check` **several times**. The first
transfer after latency changes is always slower (TCP adapting), and a
single run is not a measurement.

---

### 6. Traces don't appear in Tempo

**Means:** spans are created but don't reach Tempo.

**Diagnose:**
```
docker compose -f infra/compose/cluster-5.yml logs otel-collector | grep -iE "exporter|error" | head
curl -s "http://localhost:3200/api/search?limit=5"
```
- Only a `debug` exporter in the collector logs: it's running its
  **default config**, so ours isn't mounted. Check that
  `observability/otel/collector.yaml` exists, is spelled `.yaml`, and is
  a file, not a directory.
- Tempo returns `"traces":[]` right after startup: wait ~15s, Tempo's
  ingester needs time to become ready.

**Mitigate:** fix the config file, then `make cluster-down && make cluster-up`.

**Verify:** run a transfer, wait 5s, query `{name="transfer.fetch_file"}`
in Grafana → Explore → Tempo.

---

### 7. A dashboard panel shows "No data"

**Diagnose:**
- Prometheus → Status → Targets: are all 5 nodes `UP`?
- Was there traffic recently? Panels with `rate(...[1m])` go empty a
  minute after the last event. Generate some: `./scripts/slo-load.sh`.

**Mitigate:** if targets are down, see playbook 1.

---

### 8. Code changes don't show up in the cluster

**Means:** Docker reused a cached image layer with the old binary.

**Mitigate:**
```
make cluster-down
docker compose -f infra/compose/cluster-5.yml build --no-cache
make cluster-up
```

---

## Fault injection (testing only)

| Command | Effect | Undo |
|---|---|---|
| `make chaos-kill NODE=N` | SIGKILL the node | `make chaos-start NODE=N` |
| `make chaos-partition NODE=N` | Disconnect from the network | `make chaos-heal NODE=N` |
| `make chaos-slow NODE=N LATENCY=200ms` | Add latency to outgoing packets | `make chaos-fast NODE=N` |

Always confirm the healthy state (`make status`, `make transfer-check`)
before and after an experiment.

## Known limitations

- Dead providers are never removed from DHT records; fetches skip them
  quickly, but the records stay.
- Connections only form along the static `-dial` lists: there is no
  peer discovery, so a node that isn't in anyone's list can't join.
- `make cluster-down` wipes all node identities and shared files.