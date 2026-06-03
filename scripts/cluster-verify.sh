#!/usr/bin/env bash
set -euo pipefail

N1="http://localhost:3000"
N2="http://localhost:3001"
N3="http://localhost:3002"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

echo ""
echo "=== Stage 1 Acceptance Verification ==="
echo ""

echo "[1] Leader election"
STATUS=$(curl -sf "$N1/cluster")
LEADER=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['leader_id'])" 2>/dev/null || echo "null")
MEMBERS=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['members']))" 2>/dev/null || echo "0")
[ "$LEADER" != "null" ] && [ "$MEMBERS" -eq 3 ] \
  && pass "leader=$LEADER, 3 members visible" \
  || fail "no leader or wrong member count (leader=$LEADER, members=$MEMBERS)"

echo "[2] Write replication"
SESSION=$(curl -sf -X POST "$N1/sessions" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
curl -sf -X POST "$N1/sessions/$SESSION/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"stage1 replication test"}' > /dev/null
sleep 1
for port in 3000 3001 3002; do
  CTX=$(curl -sf "http://localhost:$port/sessions/$SESSION/context" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['context'])" 2>/dev/null || echo "")
  echo "$CTX" | grep -q "stage1 replication test" \
    && pass "node :$port has the replicated message" \
    || fail "node :$port missing replicated message"
done

echo "[3] Follower redirect"
FOLLOWER_TESTED=0
for port in 3000 3001 3002; do
  ROLE=$(curl -sf "http://localhost:$port/cluster" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
  if [ "$ROLE" = "Follower" ]; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "http://localhost:$port/sessions/$SESSION/messages" \
      -H "Content-Type: application/json" \
      -d '{"role":"user","content":"follower write"}')
    [ "$HTTP_CODE" = "307" ] \
      && pass "follower on :$port returned 307 redirect" \
      || fail "follower on :$port returned $HTTP_CODE (expected 307)"
    FOLLOWER_TESTED=1
    break
  fi
done
[ "$FOLLOWER_TESTED" -eq 1 ] || fail "no follower node found — cluster may not have elected a leader yet"

echo "[4] Failover"
echo "  Stopping node-1..."
docker compose -f docker-compose.cluster.yml stop node-1
sleep 2
NEW_STATUS=$(curl -sf "$N2/cluster" 2>/dev/null || curl -sf "$N3/cluster")
NEW_LEADER=$(echo "$NEW_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['leader_id'])" 2>/dev/null || echo "null")
[ "$NEW_LEADER" != "null" ] \
  && pass "new leader elected: $NEW_LEADER" \
  || fail "no leader after killing node-1"
SESSION2=$(curl -sf -X POST "$N2/sessions" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
curl -sf -X POST "$N2/sessions/$SESSION2/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"post-failover write"}' > /dev/null \
  && pass "write accepted after failover" \
  || fail "write rejected after failover"
echo "  Restarting node-1..."
docker compose -f docker-compose.cluster.yml start node-1
sleep 2

echo "[5] Cluster observability"
METRICS=$(curl -sf "$N1/metrics")
echo "$METRICS" | grep -q "engram_raft_term"         && pass "engram_raft_term present"         || fail "engram_raft_term missing"
echo "$METRICS" | grep -q "engram_raft_commit_index" && pass "engram_raft_commit_index present" || fail "engram_raft_commit_index missing"
echo "$METRICS" | grep -q "engram_raft_is_leader"    && pass "engram_raft_is_leader present"    || fail "engram_raft_is_leader missing"

echo ""
echo "=== All Stage 1 criteria PASSED ==="
