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
WRITE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$N1/sessions/$SESSION/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"stage1 replication test"}')
[ "$WRITE_CODE" = "204" ] || fail "write to leader returned HTTP $WRITE_CODE (expected 204)"
sleep 1
# Verify replication by checking last_applied_index matches across all nodes.
# Using /cluster avoids calling OpenAI (which /context requires).
LEADER_APPLIED=$(curl -sf "$N1/cluster" | python3 -c "import sys,json; print(json.load(sys.stdin)['last_applied_index'])" 2>/dev/null || echo "null")
[ "$LEADER_APPLIED" != "null" ] || fail "could not read last_applied_index from leader"
for port in 3000 3001 3002; do
  NODE_APPLIED=$(curl -sf "http://localhost:$port/cluster" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['last_applied_index'])" 2>/dev/null || echo "null")
  [ "$NODE_APPLIED" = "$LEADER_APPLIED" ] \
    && pass "node :$port applied_index=$NODE_APPLIED (matches leader)" \
    || fail "node :$port applied_index=$NODE_APPLIED (leader has $LEADER_APPLIED)"
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
# Wait up to 5 seconds for a new leader to be elected (heartbeat=250ms, timeout max=500ms)
NEW_LEADER="null"
for i in $(seq 1 10); do
  sleep 0.5
  NEW_STATUS=$(curl -sf "$N2/cluster" 2>/dev/null || curl -sf "$N3/cluster" 2>/dev/null || echo "{}")
  NEW_LEADER=$(echo "$NEW_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('leader_id','null'))" 2>/dev/null || echo "null")
  [ "$NEW_LEADER" != "null" ] && [ "$NEW_LEADER" != "None" ] && break
done
[ "$NEW_LEADER" != "null" ] && [ "$NEW_LEADER" != "None" ] \
  && pass "new leader elected: $NEW_LEADER" \
  || fail "no leader after killing node-1"
# Write to whichever surviving node is now the leader
WRITE_NODE="$N2"
WRITE_ROLE=$(curl -sf "$N2/cluster" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
[ "$WRITE_ROLE" = "Leader" ] || WRITE_NODE="$N3"
SESSION2=$(curl -sf -X POST "$WRITE_NODE/sessions" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
curl -sf -X POST "$WRITE_NODE/sessions/$SESSION2/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"post-failover write"}' > /dev/null \
  && pass "write accepted after failover" \
  || fail "write rejected after failover"
echo "  Restarting node-1..."
docker compose -f docker-compose.cluster.yml start node-1
sleep 2

echo "[5] Cluster observability"
# Read metrics from whichever node is the current leader (node-1 may still be catching up)
METRICS_NODE="$N2"
METRICS_ROLE=$(curl -sf "$N2/cluster" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
[ "$METRICS_ROLE" = "Leader" ] || METRICS_NODE="$N3"
METRICS=$(curl -sf "$METRICS_NODE/metrics")
echo "$METRICS" | grep -q "engram_raft_term"         && pass "engram_raft_term present"         || fail "engram_raft_term missing"
echo "$METRICS" | grep -q "engram_raft_commit_index" && pass "engram_raft_commit_index present" || fail "engram_raft_commit_index missing"
echo "$METRICS" | grep -q "engram_raft_is_leader"    && pass "engram_raft_is_leader present"    || fail "engram_raft_is_leader missing"


echo "[6] Knowledge replication"
SESSION_K=$(curl -sf -X POST "$N1/sessions" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")

curl -sf -X POST "$N1/sessions/$SESSION_K/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"Alice works at OpenAI. Bob works at OpenAI. Alice knows Bob."}' > /dev/null

echo "  Waiting 5 seconds for knowledge extraction and replication..."
sleep 5

LEADER_PORT=""
for port in 3000 3001 3002; do
  ROLE=$(curl -sf "http://localhost:$port/cluster" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
  if [ "$ROLE" = "Leader" ]; then
    LEADER_PORT="$port"
    break
  fi
done

[ -z "$LEADER_PORT" ] && fail "no leader found for knowledge check"

LEADER_ENTITIES=$(curl -sf "http://localhost:$LEADER_PORT/sessions/$SESSION_K/knowledge" | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1")

[ "$LEADER_ENTITIES" -gt 0 ] \
  && pass "leader (:$LEADER_PORT) has $LEADER_ENTITIES entities" \
  || fail "leader (:$LEADER_PORT) has no entities after 5 seconds (is OPENAI_API_KEY set?)"

for port in 3000 3001 3002; do
  [ "$port" -eq "$LEADER_PORT" ] && continue
  FOLLOWER_ENTITIES=$(curl -sf "http://localhost:$port/sessions/$SESSION_K/knowledge" | \
    python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1")
  [ "$FOLLOWER_ENTITIES" -eq "$LEADER_ENTITIES" ] \
    && pass "follower :$port converged to $FOLLOWER_ENTITIES entities (matches leader)" \
    || fail "follower :$port has $FOLLOWER_ENTITIES entities (leader has $LEADER_ENTITIES)"
done

echo "[6b] Capability criterion: graph answers questions without LLM or vector search"
RELATED=$(curl -sf "http://localhost:$LEADER_PORT/sessions/$SESSION_K/knowledge/entities/OpenAI" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print([r['name'] for r in d['related']])" 2>/dev/null || echo "[]")
echo "$RELATED" | grep -q "Alice" \
  && pass "OpenAI is related to Alice (works_at)" \
  || fail "OpenAI not related to Alice"
echo "$RELATED" | grep -q "Bob" \
  && pass "OpenAI is related to Bob (works_at)" \
  || fail "OpenAI not related to Bob"

echo ""
echo "=== All Stage 1 + Stage 2 criteria PASSED ==="
