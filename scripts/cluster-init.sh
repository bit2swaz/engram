#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:3000"
echo "=== Initializing 3-node engram cluster ==="

for port in 3000 3001 3002; do
  echo -n "Waiting for node on :$port..."
  for i in $(seq 1 30); do
    if curl -sf "http://localhost:$port/health" > /dev/null 2>&1; then
      echo " ready."
      break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then echo " TIMEOUT"; exit 1; fi
  done
done

echo "Initializing cluster..."
curl -sf -X POST "$BASE/cluster/init"
echo ""
sleep 2

echo "=== Cluster status ==="
curl -s "$BASE/cluster" | python3 -m json.tool
