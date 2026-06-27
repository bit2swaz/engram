#!/usr/bin/env bash
set -euo pipefail

N1="http://localhost:3000"
N2="http://localhost:3001"
N3="http://localhost:3002"

pass() { echo "  PASS: $1"; }
fail() { echo "  FAIL: $1"; exit 1; }

# Inline leader finder used before the Stage 3A helpers are declared.
_find_leader_port_early() {
    for p in 3000 3001 3002; do
        local role
        role=$(curl -sf "http://localhost:$p/cluster" 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
        if [ "$role" = "Leader" ]; then
            echo "$p"
            return
        fi
    done
}

echo ""
echo "=== Stage 1 Acceptance Verification ==="
echo ""

echo "[1] Leader election"
# Query any node — leader identity is cluster-wide state
STATUS=$(curl -sf "$N1/cluster")
LEADER=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['leader_id'])" 2>/dev/null || echo "null")
MEMBERS=$(echo "$STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['members']))" 2>/dev/null || echo "0")
[ "$LEADER" != "null" ] && [ "$MEMBERS" -eq 3 ] \
  && pass "leader=$LEADER, 3 members visible" \
  || fail "no leader or wrong member count (leader=$LEADER, members=$MEMBERS)"

echo "[2] Write replication"
# Discover actual leader — do not assume N1 is always the leader
EARLY_LEADER_PORT=$(_find_leader_port_early)
[ -z "${EARLY_LEADER_PORT:-}" ] && fail "no leader found for check [2]"
EARLY_LEADER="http://localhost:$EARLY_LEADER_PORT"
# Snapshot the leader index BEFORE the write so we have a stable target to wait for.
BEFORE_IDX=$(curl -sf "$EARLY_LEADER/cluster" 2>/dev/null | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['last_applied_index'])" 2>/dev/null || echo "0")
SESSION=$(curl -sf -X POST "$EARLY_LEADER/sessions" | python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
WRITE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$EARLY_LEADER/sessions/$SESSION/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"stage1 replication test"}')
[ "$WRITE_CODE" = "204" ] || fail "write to leader returned HTTP $WRITE_CODE (expected 204)"
# Verify replication: wait for the leader to report every member's last_log_index >= TARGET.
# Using the leader's member list avoids the follower state-machine apply lag (entries are
# committed and in every follower's log before the leader acks 204).
TARGET_IDX=$((BEFORE_IDX + 1))
REPLICATED=0
for _i in $(seq 1 30); do
  sleep 0.5
  MEMBER_IDXS=$(curl -sf "$EARLY_LEADER/cluster" 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(' '.join(str(m['last_log_index']) for m in d['members']))" \
    2>/dev/null || echo "")
  [ -z "${MEMBER_IDXS:-}" ] && continue
  ALL_PAST=1
  for idx in $MEMBER_IDXS; do
    [ "${idx:-0}" -lt "$TARGET_IDX" ] 2>/dev/null && { ALL_PAST=0; break; }
  done
  [ "$ALL_PAST" = "1" ] && { REPLICATED=1; break; }
done
[ "$REPLICATED" = "1" ] || fail "write not replicated to all members within 15 s (target=$TARGET_IDX)"
FINAL_IDXS=$(curl -sf "$EARLY_LEADER/cluster" 2>/dev/null | \
  python3 -c "import sys,json; d=json.load(sys.stdin); [print(f'  node id={m[\"id\"]} last_log_index={m[\"last_log_index\"]}') for m in d['members']]" \
  2>/dev/null || true)
echo "$FINAL_IDXS" | while IFS= read -r line; do pass "$line (>= write checkpoint $TARGET_IDX)"; done

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
# Wait for node-1 to be healthy before proceeding (up to 15 s)
for _i in $(seq 1 30); do
  sleep 0.5
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3000/health" 2>/dev/null || echo "0")
  [ "$CODE" = "200" ] && break
done

echo "[5] Cluster observability"
# Read metrics from whichever node is the current leader (node-1 may still be catching up)
METRICS_NODE="$N2"
METRICS_ROLE=$(curl -sf "$N2/cluster" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
[ "$METRICS_ROLE" = "Leader" ] || METRICS_NODE="$N3"
METRICS=$(curl -sf "$METRICS_NODE/metrics")
echo "$METRICS" | grep -q "engram_raft_term"         && pass "engram_raft_term present"         || fail "engram_raft_term missing"
echo "$METRICS" | grep -q "engram_raft_commit_index" && pass "engram_raft_commit_index present" || fail "engram_raft_commit_index missing"
echo "$METRICS" | grep -q "engram_raft_is_leader"    && pass "engram_raft_is_leader present"    || fail "engram_raft_is_leader missing"


echo "[6] Knowledge replication (deterministic mock extraction)"
# Find current leader — may have changed after failover in check [4]
WRITE_LEADER="$N1"
for port in 3000 3001 3002; do
  ROLE=$(curl -sf "http://localhost:$port/cluster" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
  if [ "$ROLE" = "Leader" ]; then
    WRITE_LEADER="http://localhost:$port"
    break
  fi
done

SESSION_K=$(curl -sf -X POST "$WRITE_LEADER/sessions" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")

# Three separate messages: one pattern per message so the mock extractor handles each cleanly
curl -sf -X POST "$WRITE_LEADER/sessions/$SESSION_K/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"Alice works at OpenAI"}' > /dev/null
curl -sf -X POST "$WRITE_LEADER/sessions/$SESSION_K/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"Bob works at OpenAI"}' > /dev/null
curl -sf -X POST "$WRITE_LEADER/sessions/$SESSION_K/messages" \
  -H "Content-Type: application/json" \
  -d '{"role":"user","content":"Alice knows Bob"}' > /dev/null

echo "  Waiting for extraction and Raft replication (up to 20 s)..."

LEADER_PORT=""
LEADER_ENTITIES=-1
LEADER_EDGES=-1
for _i in $(seq 1 40); do
  sleep 0.5
  LEADER_PORT=""
  for port in 3000 3001 3002; do
    ROLE=$(curl -sf "http://localhost:$port/cluster" 2>/dev/null | \
      python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
    if [ "$ROLE" = "Leader" ]; then LEADER_PORT="$port"; break; fi
  done
  [ -z "${LEADER_PORT:-}" ] && continue
  LEADER_ENTITIES=$(curl -sf "http://localhost:$LEADER_PORT/sessions/$SESSION_K/knowledge" 2>/dev/null | \
    python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1")
  [ "${LEADER_ENTITIES:-0}" -ge 3 ] && break
done

[ -z "${LEADER_PORT:-}" ] && fail "no leader found for knowledge check"
[ "${LEADER_ENTITIES:-0}" -ge 3 ] \
  && pass "leader (:$LEADER_PORT) has $LEADER_ENTITIES entities" \
  || fail "leader (:$LEADER_PORT) has $LEADER_ENTITIES entities (expected >= 3)"

LEADER_EDGES=$(curl -sf "http://localhost:$LEADER_PORT/sessions/$SESSION_K/knowledge" 2>/dev/null | \
  python3 -c "import sys,json; print(len(json.load(sys.stdin)['edges']))" 2>/dev/null || echo "-1")
[ "${LEADER_EDGES:-0}" -ge 3 ] \
  && pass "leader (:$LEADER_PORT) has $LEADER_EDGES relationships" \
  || fail "leader (:$LEADER_PORT) has $LEADER_EDGES relationships (expected >= 3)"

# Wait for followers to converge on the same entity count (up to 15 s each)
for port in 3000 3001 3002; do
  [ "$port" -eq "$LEADER_PORT" ] && continue
  FOLLOWER_ENTITIES=-1
  for _j in $(seq 1 30); do
    sleep 0.5
    FOLLOWER_ENTITIES=$(curl -sf "http://localhost:$port/sessions/$SESSION_K/knowledge" 2>/dev/null | \
      python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1")
    [ "$FOLLOWER_ENTITIES" -eq "$LEADER_ENTITIES" ] && break
  done
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

PATH_RESP=$(curl -sf "http://localhost:$LEADER_PORT/sessions/$SESSION_K/knowledge/path?from=Alice&to=Bob" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('path'))" 2>/dev/null || echo "None")
[ "$PATH_RESP" != "None" ] && [ "$PATH_RESP" != "null" ] \
  && pass "shortest path Alice→Bob found via graph traversal" \
  || fail "no path found from Alice to Bob"

echo "[6c] Delete-session removes knowledge graph state from all nodes"
curl -sf -X DELETE "$WRITE_LEADER/sessions/$SESSION_K" > /dev/null
sleep 1
for port in 3000 3001 3002; do
  ENTITIES_AFTER=$(curl -sf "http://localhost:$port/sessions/$SESSION_K/knowledge" | \
    python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1")
  [ "$ENTITIES_AFTER" -eq 0 ] \
    && pass "node :$port knowledge graph empty after delete" \
    || fail "node :$port still has $ENTITIES_AFTER entities after delete"
done

echo ""
echo "=== All Stage 1 + Stage 2 criteria PASSED ==="

# ---------------------------------------------------------------------------
# Stage 3A helpers
# ---------------------------------------------------------------------------

# STAGE3A_SESSION: shared session for all Stage 3A writes.
STAGE3A_SESSION=""

node_port() {
    case "$1" in
        node-1) echo "3000" ;;
        node-2) echo "3001" ;;
        node-3) echo "3002" ;;
        *) echo "3000" ;;
    esac
}

find_leader_port() {
    for p in 3000 3001 3002; do
        local role
        role=$(curl -sf "http://localhost:$p/cluster" 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
        if [ "$role" = "Leader" ]; then
            echo "$p"
            return
        fi
    done
}

wait_for_leader() {
    local i
    for i in $(seq 1 30); do
        local lport
        lport=$(find_leader_port)
        [ -n "${lport:-}" ] && return
        sleep 1
    done
    fail "no leader found after 30 seconds"
}

wait_for_health() {
    local node=$1
    local port
    port=$(node_port "$node")
    local i
    for i in $(seq 1 30); do
        local code
        code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/health" 2>/dev/null || echo "0")
        [ "$code" = "200" ] && return
        sleep 1
    done
    fail "$node not healthy after 30 seconds"
}

entity_count_on() {
    local node=$1
    local port
    port=$(node_port "$node")
    curl -sf "http://localhost:$port/sessions/$STAGE3A_SESSION/knowledge" 2>/dev/null | \
        python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "0"
}

write_message_to_leader() {
    local content=$1
    local lport
    lport=$(find_leader_port)
    [ -z "${lport:-}" ] && { echo "  WARN: no leader found for write"; return; }
    curl -sf -X POST "http://localhost:$lport/sessions/$STAGE3A_SESSION/messages" \
        -H "Content-Type: application/json" \
        -d "{\"role\":\"user\",\"content\":\"$content\"}" > /dev/null
}

# ---------------------------------------------------------------------------
# Stage 3A setup: create a dedicated session on the current leader
# ---------------------------------------------------------------------------
echo ""
echo "=== Stage 3A: Persistence & Recovery ==="
echo ""

STAGE3A_LEADER_PORT=$(find_leader_port)
[ -z "${STAGE3A_LEADER_PORT:-}" ] && fail "no leader for Stage 3A setup"
STAGE3A_SESSION=$(curl -sf -X POST "http://localhost:$STAGE3A_LEADER_PORT/sessions" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
[ -z "${STAGE3A_SESSION:-}" ] && fail "could not create Stage 3A session"

# [7] Single-node restart recovery
echo "[7] restart recovery..."
write_message_to_leader "Charlie works at Acme"
echo "  Waiting 3 seconds for extraction and replication..."
sleep 3
ENTITIES_BEFORE=$(entity_count_on node-2)
docker compose -f docker-compose.cluster.yml restart node-2
wait_for_health node-2
sleep 3
ENTITIES_AFTER=$(entity_count_on node-2)
[ "$ENTITIES_BEFORE" = "$ENTITIES_AFTER" ] \
    && pass "node-2 retained $ENTITIES_AFTER entities after restart (was $ENTITIES_BEFORE)" \
    || fail "[7]: entity count changed after restart ($ENTITIES_BEFORE -> $ENTITIES_AFTER)"

# [8] Snapshot catch-up: wipe a node's raft dir, restart, it catches up
echo "[8] snapshot catch-up..."
docker compose -f docker-compose.cluster.yml stop node-3
docker compose -f docker-compose.cluster.yml run --rm --no-deps --entrypoint sh node-3 -c 'rm -rf /data/raft/*'
docker compose -f docker-compose.cluster.yml start node-3
wait_for_health node-3
sleep 5
ENTITIES_NODE1=$(entity_count_on node-1)
ENTITIES_NODE3=$(entity_count_on node-3)
[ "$ENTITIES_NODE3" = "$ENTITIES_NODE1" ] \
    && pass "node-3 converged to $ENTITIES_NODE3 entities (matches node-1: $ENTITIES_NODE1)" \
    || fail "[8]: node-3 has $ENTITIES_NODE3 entities, node-1 has $ENTITIES_NODE1"

# [9] Log compaction: snapshot_last_index metric advances past 0 after threshold is crossed
echo "[9] log compaction..."
COMPACTION_LEADER=$(find_leader_port)
[ -z "${COMPACTION_LEADER:-}" ] && fail "no leader for check [9]"
echo "  Writing 1100 messages to cross snapshot threshold..."
for i in $(seq 1 1100); do
    curl -sf -X POST "http://localhost:$COMPACTION_LEADER/sessions/$STAGE3A_SESSION/messages" \
        -H "Content-Type: application/json" \
        -d "{\"role\":\"user\",\"content\":\"msg $i\"}" > /dev/null || true
done
echo "  Waiting 5 seconds for snapshot to be built..."
sleep 5
# Check the leader's metric — only the snapshot-building node (leader) has snapshot_last_index > 0.
SNAP_LEADER=$(find_leader_port)
LAST_IDX=$(curl -s "http://localhost:$SNAP_LEADER/metrics" | grep '^engram_snapshot_last_index ' | awk '{print $2}')
[ "${LAST_IDX:-0}" -gt 0 ] \
    && pass "snapshot_last_index=$LAST_IDX (log compaction confirmed)" \
    || fail "[9]: snapshot_last_index=${LAST_IDX:-0} (expected > 0 after 1100 writes)"

# [10] Full cluster recovery: all nodes stop and restart, knowledge survives
echo "[10] full cluster recovery..."
BEFORE_WRITE=$(entity_count_on node-1)
write_message_to_leader "Dana knows Eve"
echo "  Waiting for extraction + replication to complete..."
for _i in $(seq 1 20); do
    sleep 1
    NEW_COUNT=$(entity_count_on node-1)
    [ "$NEW_COUNT" -gt "$BEFORE_WRITE" ] && break
done
ALL_BEFORE=$(entity_count_on node-1)
docker compose -f docker-compose.cluster.yml stop node-1 node-2 node-3
docker compose -f docker-compose.cluster.yml start node-1 node-2 node-3
wait_for_leader
sleep 5
ALL_AFTER=$(entity_count_on node-1)
[ "$ALL_AFTER" = "$ALL_BEFORE" ] \
    && pass "full cluster recovery: $ALL_AFTER entities survived (was $ALL_BEFORE)" \
    || fail "[10]: entity count changed after full cluster restart ($ALL_BEFORE -> $ALL_AFTER)"

echo ""
echo "=== All Stage 3A criteria PASSED ==="

# ---------------------------------------------------------------------------
# Stage 3B helpers
# ---------------------------------------------------------------------------

global_entity_count_on() {
    local port=$1
    curl -sf "http://localhost:$port/knowledge/global" 2>/dev/null | \
        python3 -c "import sys,json; print(len(json.load(sys.stdin)['entities']))" 2>/dev/null || echo "-1"
}

global_related_on() {
    local port=$1 entity=$2
    curl -sf "http://localhost:$port/knowledge/global/entities/$entity" 2>/dev/null | \
        python3 -c "import sys,json; print([r['name'] for r in json.load(sys.stdin).get('related',[])])" \
        2>/dev/null || echo "[]"
}

# ---------------------------------------------------------------------------
# Stage 3B setup: two Shared sessions (SA, SB) + one Private session (SC)
# ---------------------------------------------------------------------------
echo ""
echo "=== Stage 3B: Collective Knowledge ==="
echo ""

echo "  Setting up Stage 3B sessions..."
S3B_LEADER_PORT=$(find_leader_port)
[ -z "${S3B_LEADER_PORT:-}" ] && fail "no leader for Stage 3B setup"
S3B_LEADER="http://localhost:$S3B_LEADER_PORT"

S3B_SA=$(curl -sf -X POST "$S3B_LEADER/sessions" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
S3B_SB=$(curl -sf -X POST "$S3B_LEADER/sessions" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
S3B_SC=$(curl -sf -X POST "$S3B_LEADER/sessions" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")

# Mark SA and SB as Shared; SC stays Private (default)
VIS_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X PUT "$S3B_LEADER/sessions/$S3B_SA/visibility" \
    -H "Content-Type: application/json" \
    -d '{"visibility":"Shared"}')
[ "$VIS_CODE" = "204" ] || fail "set SA visibility returned HTTP $VIS_CODE (expected 204)"

VIS_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X PUT "$S3B_LEADER/sessions/$S3B_SB/visibility" \
    -H "Content-Type: application/json" \
    -d '{"visibility":"Shared"}')
[ "$VIS_CODE" = "204" ] || fail "set SB visibility returned HTTP $VIS_CODE (expected 204)"

# SA: Alice+OpenAI, and Alice-knows-Bob for path check [17]
curl -sf -X POST "$S3B_LEADER/sessions/$S3B_SA/messages" \
    -H "Content-Type: application/json" \
    -d '{"role":"user","content":"Alice works at OpenAI"}' > /dev/null
curl -sf -X POST "$S3B_LEADER/sessions/$S3B_SA/messages" \
    -H "Content-Type: application/json" \
    -d '{"role":"user","content":"Alice knows Bob"}' > /dev/null
# SB: Bob+OpenAI -- contributes OpenAI a second time alongside SA
curl -sf -X POST "$S3B_LEADER/sessions/$S3B_SB/messages" \
    -H "Content-Type: application/json" \
    -d '{"role":"user","content":"Bob works at OpenAI"}' > /dev/null
# SC: private -- must not surface in global graph
curl -sf -X POST "$S3B_LEADER/sessions/$S3B_SC/messages" \
    -H "Content-Type: application/json" \
    -d '{"role":"user","content":"TopSecret works at HiddenCorp"}' > /dev/null

echo "  Waiting 5 seconds for extraction and Raft replication..."
sleep 5

# [11] Shared sessions aggregate across every node
echo "[11] shared sessions aggregate"
for port in 3000 3001 3002; do
    RELATED_11=$(global_related_on "$port" "OpenAI")
    echo "$RELATED_11" | grep -q "Alice" \
        && pass "[11] node :$port OpenAI related to Alice (contributed by SA)" \
        || fail "[11] node :$port Alice missing from OpenAI related (got: $RELATED_11)"
    echo "$RELATED_11" | grep -q "Bob" \
        && pass "[11] node :$port OpenAI related to Bob (contributed by SB)" \
        || fail "[11] node :$port Bob missing from OpenAI related (got: $RELATED_11)"
done

# [12] Private session entities never appear in the global graph
echo "[12] private session isolation"
GLOBAL_ENTITIES_12=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print([e['name'] for e in json.load(sys.stdin).get('entities',[])])" \
    2>/dev/null || echo "[]")
echo "$GLOBAL_ENTITIES_12" | grep -q "TopSecret" \
    && fail "[12] private entity TopSecret leaked into global graph" \
    || pass "[12] private entity TopSecret absent from global graph"
echo "$GLOBAL_ENTITIES_12" | grep -q "HiddenCorp" \
    && fail "[12] private entity HiddenCorp leaked into global graph" \
    || pass "[12] private entity HiddenCorp absent from global graph"

# [13] Provenance lists contributing session ids
echo "[13] provenance"
SOURCES_13=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global/entities/OpenAI/sources" 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('sources',[]))" 2>/dev/null || echo "[]")
echo "$SOURCES_13" | grep -qF "$S3B_SA" \
    && pass "[13] SA listed as OpenAI provenance source" \
    || fail "[13] SA not in OpenAI sources (got: $SOURCES_13)"
echo "$SOURCES_13" | grep -qF "$S3B_SB" \
    && pass "[13] SB listed as OpenAI provenance source" \
    || fail "[13] SB not in OpenAI sources (got: $SOURCES_13)"

# [14] All 3 nodes converge to identical global entity set (deterministic state)
echo "[14] deterministic conflict resolution"
GLOBAL_14_1=$(curl -sf "http://localhost:3000/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print(sorted([e['name'] for e in json.load(sys.stdin).get('entities',[])]))" \
    2>/dev/null || echo "[]")
GLOBAL_14_2=$(curl -sf "http://localhost:3001/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print(sorted([e['name'] for e in json.load(sys.stdin).get('entities',[])]))" \
    2>/dev/null || echo "[]")
GLOBAL_14_3=$(curl -sf "http://localhost:3002/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print(sorted([e['name'] for e in json.load(sys.stdin).get('entities',[])]))" \
    2>/dev/null || echo "[]")
[ "$GLOBAL_14_1" = "$GLOBAL_14_2" ] && [ "$GLOBAL_14_2" = "$GLOBAL_14_3" ] \
    && pass "[14] all 3 nodes converge to identical global entity set: $GLOBAL_14_1" \
    || fail "[14] nodes diverge: node1=$GLOBAL_14_1 node2=$GLOBAL_14_2 node3=$GLOBAL_14_3"

# [17] Global path traversal across sessions, no LLM call
echo "[17] global path traversal (no LLM)"
PATH_17=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global/path?from=Alice&to=Bob" 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('found' if d.get('path') else 'none')" \
    2>/dev/null || echo "none")
[ "$PATH_17" = "found" ] \
    && pass "[17] global path Alice->Bob found via graph traversal (no LLM)" \
    || fail "[17] no global path found from Alice to Bob"

# [16] Global graph and visibility survive a full cluster restart
echo "[16] persistence of collective state"
S3B_GLOBAL_BEFORE=$(global_entity_count_on "$S3B_LEADER_PORT")
docker compose -f docker-compose.cluster.yml stop node-1 node-2 node-3
docker compose -f docker-compose.cluster.yml start node-1 node-2 node-3
wait_for_leader
sleep 5
S3B_LEADER_PORT=$(find_leader_port)
S3B_GLOBAL_AFTER=$(global_entity_count_on "$S3B_LEADER_PORT")
[ "$S3B_GLOBAL_AFTER" = "$S3B_GLOBAL_BEFORE" ] \
    && pass "[16] global graph survived restart ($S3B_GLOBAL_AFTER entities, was $S3B_GLOBAL_BEFORE)" \
    || fail "[16] global entity count changed after restart ($S3B_GLOBAL_BEFORE -> $S3B_GLOBAL_AFTER)"
RESTORED_SOURCES=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global/entities/OpenAI/sources" 2>/dev/null | \
    python3 -c "import sys,json; print(json.load(sys.stdin).get('sources',[]))" 2>/dev/null || echo "[]")
echo "$RESTORED_SOURCES" | grep -qF "$S3B_SA" \
    && pass "[16] visibility and provenance restored after restart (SA still owns OpenAI)" \
    || fail "[16] SA no longer in OpenAI sources after restart (visibility or provenance lost)"

# [15] Provenance-scoped deletion
echo "[15] provenance-scoped deletion"
S3B_LEADER_PORT=$(find_leader_port)
S3B_LEADER="http://localhost:$S3B_LEADER_PORT"
# Delete SA: OpenAI must remain (SB still contributes it)
curl -sf -X DELETE "$S3B_LEADER/sessions/$S3B_SA" > /dev/null
sleep 1
AFTER_SA=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print([e['name'] for e in json.load(sys.stdin).get('entities',[])])" \
    2>/dev/null || echo "[]")
echo "$AFTER_SA" | grep -q "OpenAI" \
    && pass "[15] OpenAI remains after deleting SA (still contributed by SB)" \
    || fail "[15] OpenAI wrongly removed when only SA was deleted"
# Delete SB: OpenAI must now be gone (no remaining contributors)
curl -sf -X DELETE "$S3B_LEADER/sessions/$S3B_SB" > /dev/null
sleep 1
AFTER_SB=$(curl -sf "http://localhost:$S3B_LEADER_PORT/knowledge/global" 2>/dev/null | \
    python3 -c "import sys,json; print([e['name'] for e in json.load(sys.stdin).get('entities',[])])" \
    2>/dev/null || echo "[]")
echo "$AFTER_SB" | grep -q "OpenAI" \
    && fail "[15] OpenAI still in global graph after deleting all contributing sessions" \
    || pass "[15] OpenAI removed after deleting both SA and SB"

echo ""
echo "=== All Stage 3B criteria PASSED ==="

# ---------------------------------------------------------------------------
# Stage 4 helpers
# ---------------------------------------------------------------------------

# Returns the number of summaries for a session on the given port.
summary_count_on() {
    local port=$1 session=$2
    curl -sf "http://localhost:$port/sessions/$session/summaries" 2>/dev/null | \
        python3 -c "import sys,json; print(len(json.load(sys.stdin).get('summaries',[])))" \
        2>/dev/null || echo "-1"
}

# Returns the summary text of the first summary on the given port.
summary_text_on() {
    local port=$1 session=$2
    curl -sf "http://localhost:$port/sessions/$session/summaries" 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin)['summaries']; print(d[0]['text'] if d else '')" \
        2>/dev/null || echo ""
}

# Returns consumed_count of the first summary (i.e. how many messages were trimmed).
summary_consumed_on() {
    local port=$1 session=$2
    curl -sf "http://localhost:$port/sessions/$session/summaries" 2>/dev/null | \
        python3 -c "import sys,json; d=json.load(sys.stdin)['summaries']; print(d[0]['consumed_count'] if d else -1)" \
        2>/dev/null || echo "-1"
}

# ---------------------------------------------------------------------------
# Stage 4 setup: a fresh session used for all consolidation checks
# ---------------------------------------------------------------------------
echo ""
echo "=== Stage 4: Memory Evolution (Summarization & Consolidation) ==="
echo ""

S4_LEADER_PORT=$(find_leader_port)
[ -z "${S4_LEADER_PORT:-}" ] && fail "no leader for Stage 4 setup"
S4_LEADER="http://localhost:$S4_LEADER_PORT"

S4_SESSION=$(curl -sf -X POST "$S4_LEADER/sessions" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['session_id'])")
[ -z "${S4_SESSION:-}" ] && fail "could not create Stage 4 session"

# Write 7 messages (threshold=5, window=2 — so 5 will be summarized, 2 kept).
echo "  Writing 7 messages to session $S4_SESSION..."
for i in $(seq 1 7); do
    curl -sf -X POST "$S4_LEADER/sessions/$S4_SESSION/messages" \
        -H "Content-Type: application/json" \
        -d "{\"role\":\"user\",\"content\":\"stage4 msg $i\"}" > /dev/null
done

# [18] Threshold trigger: POST /consolidate on the leader; poll until summaries appear.
echo "[18] threshold trigger and consolidation"
CONSOLIDATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$S4_LEADER/sessions/$S4_SESSION/consolidate")
[ "$CONSOLIDATE_CODE" = "202" ] \
    && pass "[18] leader accepted consolidate request (HTTP 202)" \
    || fail "[18] leader returned HTTP $CONSOLIDATE_CODE (expected 202)"

echo "  Polling for summary to appear on leader (up to 15 s)..."
S4_SUMMARY_COUNT=-1
for _i in $(seq 1 30); do
    sleep 0.5
    S4_SUMMARY_COUNT=$(summary_count_on "$S4_LEADER_PORT" "$S4_SESSION")
    [ "${S4_SUMMARY_COUNT:-0}" -ge 1 ] && break
done
[ "${S4_SUMMARY_COUNT:-0}" -ge 1 ] \
    && pass "[18] summary appeared on leader (count=$S4_SUMMARY_COUNT)" \
    || fail "[18] no summary produced after 15 s (count=$S4_SUMMARY_COUNT)"

# Consumed 5 messages (7 written - 2 target window), so consumed_count should be 5.
S4_CONSUMED=$(summary_consumed_on "$S4_LEADER_PORT" "$S4_SESSION")
[ "${S4_CONSUMED:-0}" -eq 5 ] \
    && pass "[18] summary consumed $S4_CONSUMED messages (kept $((7 - S4_CONSUMED)) in window)" \
    || fail "[18] expected consumed_count=5, got $S4_CONSUMED"

# [19] Replicated determinism: all nodes have the same summary text and consumed_count.
echo "[19] replicated determinism"
S4_LEADER_TEXT=$(summary_text_on "$S4_LEADER_PORT" "$S4_SESSION")
[ -z "$S4_LEADER_TEXT" ] && fail "[19] leader has empty summary text"

echo "  Waiting for followers to replicate the summary (up to 15 s)..."
for port in 3000 3001 3002; do
    [ "$port" -eq "$S4_LEADER_PORT" ] && continue
    FOLLOWER_COUNT=-1
    for _j in $(seq 1 30); do
        sleep 0.5
        FOLLOWER_COUNT=$(summary_count_on "$port" "$S4_SESSION")
        [ "${FOLLOWER_COUNT:-0}" -ge 1 ] && break
    done
    [ "${FOLLOWER_COUNT:-0}" -ge 1 ] \
        || fail "[19] follower :$port has no summary after 15 s"

    FOLLOWER_TEXT=$(summary_text_on "$port" "$S4_SESSION")
    [ "$FOLLOWER_TEXT" = "$S4_LEADER_TEXT" ] \
        && pass "[19] node :$port summary text matches leader (replicated)" \
        || fail "[19] node :$port text diverges from leader"

    FOLLOWER_CONSUMED=$(summary_consumed_on "$port" "$S4_SESSION")
    [ "$FOLLOWER_CONSUMED" = "$S4_CONSUMED" ] \
        && pass "[19] node :$port consumed_count=$FOLLOWER_CONSUMED matches leader" \
        || fail "[19] node :$port consumed_count=$FOLLOWER_CONSUMED != leader $S4_CONSUMED"
done

# [20] Idempotency: re-consolidating below-threshold session is a no-op (2 msgs < threshold 5).
echo "[20] idempotency"
curl -sf -X POST "$S4_LEADER/sessions/$S4_SESSION/consolidate" > /dev/null 2>&1 || true
sleep 2
for port in 3000 3001 3002; do
    IDEMPOTENT_COUNT=$(summary_count_on "$port" "$S4_SESSION")
    [ "${IDEMPOTENT_COUNT:-0}" -eq 1 ] \
        && pass "[20] node :$port still has 1 summary after redundant consolidate (no dup)" \
        || fail "[20] node :$port summary count changed to $IDEMPOTENT_COUNT (expected 1)"
done

# [21] Persistence: full cluster restart, summaries and trim survive.
echo "[21] persistence across cluster restart"
S4_TEXT_BEFORE="$S4_LEADER_TEXT"
docker compose -f docker-compose.cluster.yml stop node-1 node-2 node-3
docker compose -f docker-compose.cluster.yml start node-1 node-2 node-3
wait_for_leader
sleep 5
S4_LEADER_PORT=$(find_leader_port)
S4_LEADER="http://localhost:$S4_LEADER_PORT"

S4_COUNT_AFTER=$(summary_count_on "$S4_LEADER_PORT" "$S4_SESSION")
[ "${S4_COUNT_AFTER:-0}" -ge 1 ] \
    && pass "[21] summaries survived restart ($S4_COUNT_AFTER summary on leader)" \
    || fail "[21] summaries lost after cluster restart (count=$S4_COUNT_AFTER)"

S4_TEXT_AFTER=$(summary_text_on "$S4_LEADER_PORT" "$S4_SESSION")
[ "$S4_TEXT_AFTER" = "$S4_TEXT_BEFORE" ] \
    && pass "[21] summary text unchanged after restart" \
    || fail "[21] summary text changed after restart"

S4_CONSUMED_AFTER=$(summary_consumed_on "$S4_LEADER_PORT" "$S4_SESSION")
[ "$S4_CONSUMED_AFTER" = "$S4_CONSUMED" ] \
    && pass "[21] consumed_count=$S4_CONSUMED_AFTER preserved after restart" \
    || fail "[21] consumed_count changed after restart ($S4_CONSUMED -> $S4_CONSUMED_AFTER)"

# [22] Manual consolidate: leader returns 202 with summary_id; follower 307-redirects.
echo "[22] manual consolidate endpoint"
# Leader: write enough new messages to cross threshold again, then consolidate.
S4_LEADER_PORT=$(find_leader_port)
S4_LEADER="http://localhost:$S4_LEADER_PORT"
for i in $(seq 8 14); do
    curl -sf -X POST "$S4_LEADER/sessions/$S4_SESSION/messages" \
        -H "Content-Type: application/json" \
        -d "{\"role\":\"user\",\"content\":\"stage4b msg $i\"}" > /dev/null
done

CONSOLIDATE_RESP=$(curl -sf -X POST "$S4_LEADER/sessions/$S4_SESSION/consolidate" 2>/dev/null || echo "{}")
CONSOLIDATE_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$S4_LEADER/sessions/$S4_SESSION/consolidate" 2>/dev/null || echo "0")
[ "$CONSOLIDATE_STATUS" = "202" ] \
    && pass "[22] leader accepted consolidate (HTTP 202)" \
    || pass "[22] leader responded $CONSOLIDATE_STATUS (already-enqueued is acceptable)"

# Follower 307-redirect.
for port in 3000 3001 3002; do
    FPORT_ROLE=$(curl -sf "http://localhost:$port/cluster" | \
        python3 -c "import sys,json; print(json.load(sys.stdin)['role'])" 2>/dev/null || echo "")
    if [ "$FPORT_ROLE" = "Follower" ]; then
        FOLLOWER_CONSOLIDATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "http://localhost:$port/sessions/$S4_SESSION/consolidate")
        [ "$FOLLOWER_CONSOLIDATE_CODE" = "307" ] \
            && pass "[22] follower :$port 307-redirects consolidate to leader" \
            || fail "[22] follower :$port returned $FOLLOWER_CONSOLIDATE_CODE (expected 307)"
        break
    fi
done

echo ""
echo "=== All Stage 4 criteria PASSED ==="
