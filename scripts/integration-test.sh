#!/usr/bin/env bash
# Integration tests against a running 5-node cluster.
# Each check polls with a timeout instead of sleeping a fixed time,
# so results don't depend on how fast the machine is.
set -euo pipefail

pass() { echo "✅ $1"; }
fail() { echo "❌ $1"; exit 1; }

# Connection count of node $1, or -1 if it doesn't answer
conns() {
  { curl -s --max-time 1 "localhost:910$1/metrics" || true; } \
    | awk '/^p2p_connections_active/ {print $2; f=1} END {if (!f) print -1}'
}

# Gossip messages delivered to node $1
delivered() {
  { curl -s --max-time 1 "localhost:910$1/metrics" || true; } \
    | awk '/^p2p_gossip_delivery_seconds_count/ {print $2; f=1} END {if (!f) print 0}'
}

# all_have WANT NODES...: true if every listed node has WANT connections
all_have() {
  local want=$1; shift
  for n in "$@"; do [ "$(conns "$n")" = "$want" ] || return 1; done
}

all_delivered() {
  for n in 2 3 4 5; do [ "$(delivered "$n")" -ge 1 ] || return 1; done
}

# wait_for SECONDS COMMAND...: retry COMMAND every second until it succeeds
wait_for() {
  local timeout=$1; shift
  for ((i = 0; i < timeout; i++)); do
    if "$@"; then return 0; fi
    sleep 1
  done
  return 1
}

# fetch_ok ROOT NODE FILE: fetch on NODE and compare with the original FILE
fetch_ok() {
  local code
  code=$(curl -s --max-time 30 -o /tmp/it.out -w '%{http_code}' "localhost:910$2/fetch/$1")
  [ "$code" = "200" ] || fail "fetch on node$2: HTTP $code: $(cat /tmp/it.out)"
  cmp -s "$3" /tmp/it.out || fail "file fetched on node$2 differs from the original"
}

echo "== 1. Mesh forms"
wait_for 60 all_have 4 1 2 3 4 5 || fail "full mesh did not form within 60s"
pass "all 5 nodes have 4 connections"

echo "== 2. Gossip reaches every node"
curl -s -X POST --data "integration test" localhost:9101/gossip > /dev/null
wait_for 10 all_delivered || fail "gossip did not reach every node within 10s"
pass "message from node1 delivered to nodes 2-5"

echo "== 3. File transfer"
head -c 10M /dev/urandom > /tmp/it.bin
root=$(curl -s --max-time 30 --data-binary @/tmp/it.bin localhost:9101/share)
[ ${#root} -eq 64 ] || fail "share returned '$root'"
fetch_ok "$root" 5 /tmp/it.bin
pass "10MB shared on node1, fetched intact on node5"

echo "== 4. Crash detection and recovery"
docker kill p2p-lab-node3-1 > /dev/null
wait_for 15 all_have 3 1 2 4 5 || fail "survivors did not detect node3's crash"
docker start p2p-lab-node3-1 > /dev/null
start=$(date +%s)
wait_for 10 all_have 4 1 2 3 4 5 || fail "mesh did not recover within 10s (recovery SLO)"
pass "node3 crash detected, mesh recovered in $(( $(date +%s) - start ))s"

echo "== 5. File survives losing its original provider"
head -c 5M /dev/urandom > /tmp/it2.bin
root=$(curl -s --max-time 30 --data-binary @/tmp/it2.bin localhost:9101/share)
fetch_ok "$root" 4 /tmp/it2.bin              # node4 becomes a seeder
sleep 3                                      # let node4 announce itself
docker kill p2p-lab-node1-1 > /dev/null
fetch_ok "$root" 5 /tmp/it2.bin
pass "file fetched on node5 after node1 was killed"

echo
echo "All integration tests passed"