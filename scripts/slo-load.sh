#!/usr/bin/env bash
# Generates SLO-relevant traffic on the local cluster:
# gossip messages, DHT lookups (every share runs several) and
# multi-source transfers. Run it, then read the SLOs dashboard.
set -euo pipefail

ROUNDS=${ROUNDS:-10}
GOSSIP=${GOSSIP:-200}

echo "== gossip: $GOSSIP messages from random nodes"
for i in $(seq 1 "$GOSSIP"); do
  node=$(( RANDOM % 5 + 1 ))
  curl -s -X POST --data "slo msg $i" "http://localhost:910$node/gossip" > /dev/null
  sleep 0.05
done

echo "== transfers: $ROUNDS rounds, final fetch with 4 sources"
for r in $(seq 1 "$ROUNDS"); do
  head -c 10M /dev/urandom > /tmp/slo.bin
  root=$(curl -s --data-binary @/tmp/slo.bin http://localhost:9101/share)

  # nodes 2-4 download first and become seeders
  for n in 2 3 4; do
    curl -s -o /dev/null "http://localhost:910$n/fetch/$root"
  done
  sleep 3   # let the seeders announce themselves in the DHT

  # node5 fetches with 4 providers available (nodes 1-4)
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:9105/fetch/$root")
  echo "round $r: node5 HTTP $code"
done

echo "done"