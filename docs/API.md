# API Reference

This document describes every REST endpoint exposed by Engram. All endpoints are served at `http://localhost:3000` by default.

| method | path                                                          | description                                           |
|--------|---------------------------------------------------------------|-------------------------------------------------------|
| POST   | /sessions                                                     | create a new session (optional agent_id)              |
| POST   | /sessions/{session_id}/messages                               | add a message                                         |
| GET    | /sessions/{session_id}/context                                | get assembled context                                 |
| POST   | /sessions/{session_id}/search                                 | semantic search over long-term memory                 |
| DELETE | /sessions/{session_id}                                        | delete a session and all its memories                 |
| PUT    | /sessions/{session_id}/core-memory                            | add a core memory fact                                |
| PUT    | /sessions/{session_id}/visibility                             | set session visibility (Private or Shared)            |
| GET    | /sessions/{session_id}/knowledge                              | get full knowledge graph for the session              |
| GET    | /sessions/{session_id}/knowledge/entities/{entity_name}       | get all entities connected to a named entity          |
| GET    | /sessions/{session_id}/knowledge/path                         | find shortest path between two entities               |
| GET    | /sessions/{session_id}/knowledge/export                       | export knowledge graph (JSON or Graphviz DOT)         |
| GET    | /knowledge/global                                             | get the cross-session global knowledge graph          |
| GET    | /knowledge/global/entities/{name}                             | get entities connected to a named entity (global)     |
| GET    | /knowledge/global/entities/{name}/sources                     | get sessions that contributed a named entity          |
| GET    | /knowledge/global/path                                        | find shortest path in the global graph                |
| GET    | /knowledge/global/export                                      | export global graph (JSON or Graphviz DOT)            |
| GET    | /knowledge/global/conflicts                                   | list conflicting facts across sessions                |
| GET    | /sessions/{session_id}/summaries                              | list consolidated summaries for a session             |
| POST   | /sessions/{session_id}/consolidate                            | manually trigger consolidation (leader only)          |
| GET    | /health                                                       | health check                                          |
| GET    | /metrics                                                      | Prometheus metrics                                    |
| GET    | /api-docs/openapi.json                                        | OpenAPI specification                                 |
| GET    | /swagger-ui/                                                  | Swagger UI                                            |
| GET    | /cluster                                                      | cluster status (cluster mode only)                    |
| POST   | /cluster/init                                                 | initialize the Raft cluster                           |
| POST   | /cluster/add-learner                                          | add a learner node                                    |
| POST   | /cluster/change-membership                                    | promote learners to full voting members               |

---

## POST /sessions

Create a new session. Optionally supply an `agent_id` to associate the session with a named agent. In cluster mode, a non-empty `agent_id` is persisted via a `RegisterSession` Raft command so the mapping survives restarts.

**request body:** optional
```json
{
  "agent_id": "agent-42"
}
```

**success response:**
- status: 200
- body:
```json
{
  "session_id": "b1e2c3d4-5678-1234-9abc-def012345678"
}
```

**error responses:**
- 500: internal server error

**examples:**
```sh
# no agent association
curl -X POST http://localhost:3000/sessions

# with agent association
curl -X POST http://localhost:3000/sessions \
  -H 'content-type: application/json' \
  -d '{"agent_id":"agent-42"}'
```

---

## POST /sessions/{session_id}/messages

Add a message to a session.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "id": "optional-client-generated-uuid",
  "role": "user",
  "content": "hello, what is rust?"
}
```

`id` is optional. `role` and `content` are required.

**success response:**
- status: 204 (no content)

**error responses:**
- 400: invalid request body
- 422: missing required fields
- 500: failed to store the message
- 503: embedding queue unavailable

**example:**
```sh
curl -X POST http://localhost:3000/sessions/{session_id}/messages \
  -H 'content-type: application/json' \
  -d '{"role":"user","content":"hello, what is rust?"}'
```

---

## GET /sessions/{session_id}/context

Get the assembled context for a session.

**path parameters:**
- `session_id` (string): session identifier

**query parameters:**
- `max_tokens` (integer, optional, default: 8000): max tokens for context
- `similarity_threshold` (float, optional, default: 0.7): min similarity for long-term memories
- `long_term_top_k` (integer, optional, default: 10): max number of long-term memories

**success response:**
- status: 200
- body:
```json
{
  "context": "core memories:\n- user name is alex\n\nconversation:\nuser: hello\n..."
}
```

**error responses:**
- 400: invalid query parameters
- 404: session not found
- 500: failed to assemble context

**example:**
```sh
curl http://localhost:3000/sessions/{session_id}/context
```

---

## POST /sessions/{session_id}/search

Semantic search over long-term memory.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "query": "rust async",
  "top_k": 5
}
```

Both `query` and `top_k` are required.

**success response:**
- status: 200
- body:
```json
{
  "results": [
    { "text": "...", "score": 0.92 }
  ]
}
```

**error responses:**
- 400: invalid request body
- 500: failed to search session memory

**example:**
```sh
curl -X POST http://localhost:3000/sessions/{session_id}/search \
  -H 'content-type: application/json' \
  -d '{"query":"rust async","top_k":5}'
```

---

## DELETE /sessions/{session_id}

Delete a session and all its memories.

**path parameters:**
- `session_id` (string): session identifier

**success response:**
- status: 204 (no content)

**error responses:**
- 500: failed to delete session

**example:**
```sh
curl -X DELETE http://localhost:3000/sessions/{session_id}
```

---

## PUT /sessions/{session_id}/core-memory

Add a core memory fact to a session.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "fact": "user prefers dark mode"
}
```

`fact` is required and must not be empty.

**success response:**
- status: 204 (no content)

**error responses:**
- 400: missing or empty fact
- 500: failed to add core memory

**example:**
```sh
curl -X PUT http://localhost:3000/sessions/{session_id}/core-memory \
  -H 'content-type: application/json' \
  -d '{"fact":"user prefers dark mode"}'
```

---

## PUT /sessions/{session_id}/visibility

Set the visibility of a session. Sessions are `Private` by default. Setting a session to `Shared` causes its extracted knowledge to be merged into the global knowledge graph.

In cluster mode this goes through Raft so all nodes see the same visibility state. In standalone mode the command is accepted and no-ops.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "visibility": "Shared"
}
```

`visibility` must be `"Shared"` or `"Private"`.

**success response:**
- status: 204 (no content)

**error responses:**
- 400: invalid request body
- 307: redirect to leader (cluster mode, follower received the request)
- 500: failed to set visibility

**example:**
```sh
curl -X PUT http://localhost:3000/sessions/{session_id}/visibility \
  -H 'content-type: application/json' \
  -d '{"visibility":"Shared"}'
```

---

## GET /health

Health check endpoint.

**success response:**
- status: 200

**example:**
```sh
curl http://localhost:3000/health
```

---

## GET /metrics

Prometheus metrics endpoint.

**success response:**
- status: 200
- body: prometheus text format

**example:**
```sh
curl http://localhost:3000/metrics
```

---

## GET /api-docs/openapi.json

OpenAPI specification (JSON).

**success response:**
- status: 200
- body: openapi json

**example:**
```sh
curl http://localhost:3000/api-docs/openapi.json | jq
```

---

## GET /swagger-ui/

Swagger UI for interactive API docs.

**success response:**
- status: 200
- body: html

**example:**
```sh
curl http://localhost:3000/swagger-ui/
```

---

## cluster endpoints

These endpoints are only available when the node is started in cluster mode (i.e., `NODE_ID` is set). Standalone nodes return 503.

---

## GET /cluster

Returns the current Raft cluster status for this node.

**success response:**
- status: 200
- body:
```json
{
  "node_id": 1,
  "role": "Leader",
  "leader_id": 1,
  "term": 3,
  "last_applied_index": 12,
  "members": [
    { "id": 1, "addr": "node-1:9001", "last_log_index": 12 },
    { "id": 2, "addr": "node-2:9001", "last_log_index": 12 },
    { "id": 3, "addr": "node-3:9001", "last_log_index": 11 }
  ]
}
```

**error responses:**
- 503: cluster mode not enabled (NODE_ID not set)

**example:**
```sh
curl http://localhost:3000/cluster
```

---

## POST /cluster/init

Initializes the Raft cluster. Call this once from any node after all nodes are running. Reads `NODE_ID`, `RAFT_ADDR` (or `RAFT_ADVERTISE_ADDR`), and `CLUSTER_PEERS` from the node's environment to build the initial membership set.

**request body:** none

**success response:**
- status: 200

**error responses:**
- 500: cluster initialization failed (e.g., already initialized)
- 503: cluster mode not enabled

**example:**
```sh
curl -X POST http://localhost:3000/cluster/init
```

---

## POST /cluster/add-learner

Adds a new node as a learner. Learners receive log replication but do not vote in elections. Promote to a full member with `/cluster/change-membership`.

**request body:**
```json
{ "node_id": 4, "addr": "node-4:9001" }
```

**success response:**
- status: 200

**error responses:**
- 500: failed to add learner
- 503: cluster mode not enabled

**example:**
```sh
curl -X POST http://localhost:3000/cluster/add-learner \
  -H 'content-type: application/json' \
  -d '{"node_id":4,"addr":"node-4:9001"}'
```

---

## POST /cluster/change-membership

Changes the cluster membership to the given set of node IDs. Nodes in the new set that are currently learners are promoted to full members; nodes not in the new set are removed.

**request body:**
```json
{ "members": [1, 2, 3] }
```

**success response:**
- status: 200

**error responses:**
- 500: membership change failed
- 503: cluster mode not enabled

**example:**
```sh
curl -X POST http://localhost:3000/cluster/change-membership \
  -H 'content-type: application/json' \
  -d '{"members":[1,2,3,4]}'
```

---

## knowledge graph endpoints

These endpoints query the per-session knowledge graph built by the background knowledge extraction pipeline. Entities and relationships are extracted automatically from every message added to the session. Results reflect all committed messages; there may be a brief delay while extraction jobs are processed.

---

## GET /sessions/{session_id}/knowledge

Returns all entities and edges in the session's knowledge graph.

**path parameters:**
- `session_id` (string): session identifier

**success response:**
- status: 200
- body:
```json
{
  "session_id": "abc123",
  "entities": [
    { "name": "Alice", "entity_type": "Person", "attributes": {} },
    { "name": "OpenAI", "entity_type": "Organization", "attributes": {} }
  ],
  "edges": [
    { "from": "Alice", "to": "OpenAI", "relationship_type": "works_at" }
  ]
}
```

**example:**
```sh
curl http://localhost:3000/sessions/{session_id}/knowledge
```

---

## GET /sessions/{session_id}/knowledge/entities/{entity_name}

Returns all entities directly connected to the named entity, including both incoming and outgoing relationships.

**path parameters:**
- `session_id` (string): session identifier
- `entity_name` (string): the entity to look up

**success response:**
- status: 200
- body:
```json
{
  "entity_name": "Alice",
  "related": [
    {
      "name": "OpenAI",
      "entity_type": "Organization",
      "relationship_type": "works_at",
      "direction": "Outgoing"
    }
  ]
}
```

**error responses:**
- 404: entity not found in this session's graph

**example:**
```sh
curl http://localhost:3000/sessions/{session_id}/knowledge/entities/Alice
```

---

## GET /sessions/{session_id}/knowledge/path

Finds the shortest directed path (BFS over outgoing edges) between two named entities.

**path parameters:**
- `session_id` (string): session identifier

**query parameters:**
- `from` (string, required): source entity name
- `to` (string, required): target entity name

**success response:**
- status: 200
- body (`path` is `null` if no path exists):
```json
{
  "from": "Alice",
  "to": "Bob",
  "path": [
    { "from": "Alice", "relationship_type": "works_at", "to": "OpenAI" },
    { "from": "OpenAI", "relationship_type": "employs", "to": "Bob" }
  ]
}
```

**example:**
```sh
curl "http://localhost:3000/sessions/{session_id}/knowledge/path?from=Alice&to=Bob"
```

---

## GET /sessions/{session_id}/knowledge/export

Exports the knowledge graph in JSON or Graphviz DOT format.

**path parameters:**
- `session_id` (string): session identifier

**query parameters:**
- `format` (string, optional, default: `json`): `json` or `dot`

**success response:**
- status: 200
- content-type: `application/json` for JSON, `text/vnd.graphviz` for DOT
- body (JSON):
```json
{
  "session_id": "abc123",
  "entities": [...],
  "edges": [...]
}
```
- body (DOT):
```
digraph knowledge {
  "Alice" [label="Alice\n(Person)"];
  "OpenAI" [label="OpenAI\n(Organization)"];
  "Alice" -> "OpenAI" [label="works_at"];
}
```

**example:**
```sh
curl "http://localhost:3000/sessions/{session_id}/knowledge/export?format=dot"
```

---

## global knowledge graph endpoints

The global graph aggregates entities and relationships from all sessions whose visibility is set to `Shared`. These endpoints return data from the local in-memory global graph. In cluster mode the global graph is eventually consistent with the leader.

---

## GET /knowledge/global

Returns all entities and edges in the global knowledge graph.

**success response:**
- status: 200
- body:
```json
{
  "entities": [
    { "name": "Alice", "entity_type": "Person", "attributes": {} },
    { "name": "OpenAI", "entity_type": "Organization", "attributes": {} }
  ],
  "edges": [
    { "from": "Alice", "to": "OpenAI", "relationship_type": "works_at" }
  ]
}
```

**example:**
```sh
curl http://localhost:3000/knowledge/global
```

---

## GET /knowledge/global/entities/{name}

Returns all entities directly connected to the named entity in the global graph, including both incoming and outgoing relationships.

**path parameters:**
- `name` (string): the entity to look up

**success response:**
- status: 200
- body:
```json
{
  "entity_name": "Alice",
  "related": [
    {
      "name": "OpenAI",
      "entity_type": "Organization",
      "relationship_type": "works_at",
      "direction": "Outgoing"
    }
  ]
}
```

**example:**
```sh
curl http://localhost:3000/knowledge/global/entities/Alice
```

---

## GET /knowledge/global/entities/{name}/sources

Returns the list of session IDs that contributed the named entity to the global graph.

**path parameters:**
- `name` (string): the entity to look up

**success response:**
- status: 200
- body:
```json
{
  "entity_name": "Alice",
  "sources": ["session-abc123", "session-def456"]
}
```

**example:**
```sh
curl http://localhost:3000/knowledge/global/entities/Alice/sources
```

---

## GET /knowledge/global/path

Finds the shortest directed path (BFS over outgoing edges) between two named entities in the global graph.

**query parameters:**
- `from` (string, required): source entity name
- `to` (string, required): target entity name

**success response:**
- status: 200
- body (`path` is `null` if no path exists):
```json
{
  "from": "Alice",
  "to": "Bob",
  "path": [
    { "from": "Alice", "relationship_type": "works_at", "to": "OpenAI" },
    { "from": "OpenAI", "relationship_type": "employs", "to": "Bob" }
  ]
}
```

**example:**
```sh
curl "http://localhost:3000/knowledge/global/path?from=Alice&to=Bob"
```

---

## GET /knowledge/global/export

Exports the global knowledge graph in JSON or Graphviz DOT format.

**query parameters:**
- `format` (string, optional, default: `json`): `json` or `dot`

**success response:**
- status: 200
- content-type: `application/json` for JSON, `text/vnd.graphviz` for DOT

**example:**
```sh
curl "http://localhost:3000/knowledge/global/export?format=dot"
```

---

## GET /knowledge/global/conflicts

Returns all detected conflicts in the global graph. A conflict occurs when two different sessions report different relationship types between the same pair of entities.

**success response:**
- status: 200
- body:
```json
{
  "conflicts": [
    {
      "entity": "Alice",
      "related": "OpenAI",
      "relationship_types": ["works_at", "founded"],
      "sessions": ["session-abc123", "session-def456"]
    }
  ]
}
```

**example:**
```sh
curl http://localhost:3000/knowledge/global/conflicts
```

---

## consolidation endpoints

These endpoints give access to the consolidated memory produced by the leader's summarization scheduler. When a session's short-term message count exceeds `CONSOLIDATION_THRESHOLD`, the leader summarizes the oldest messages, replicates the result as an `ApplySummary` command, and every node atomically stores the summary and trims the consumed raw messages.

---

## GET /sessions/{session_id}/summaries

Returns all consolidated summaries for a session, ordered by Raft log index.

**path parameters:**
- `session_id` (string): session identifier

**success response:**
- status: 200
- body:
```json
{
  "session_id": "abc123",
  "summaries": [
    {
      "id": "11111111-1111-1111-1111-111111111111",
      "text": "Alice discussed her role at OpenAI and her preference for Rust.",
      "created_at_index": 72,
      "consumed_message_ids": ["m1", "m2", "m3"],
      "consumed_count": 3,
      "model": "gpt-4o-mini",
      "prompt_version": "summarize_v1"
    }
  ]
}
```

**error responses:**
- 500: failed to retrieve summaries

**example:**
```sh
curl http://localhost:3000/sessions/{session_id}/summaries
```

---

## POST /sessions/{session_id}/consolidate

Manually triggers consolidation for a session. The leader summarizes all messages beyond `CONSOLIDATION_TARGET_WINDOW`, stores the result, and trims the consumed raw messages. Useful for debugging, verification, and reproducible cluster tests.

In cluster mode, followers return 307 with a `Location` header pointing to the leader.

**path parameters:**
- `session_id` (string): session identifier

**request body:** none

**success response:**
- status: 202 (accepted)
- body:
```json
{
  "summary_id": "11111111-1111-1111-1111-111111111111"
}
```

**error responses:**
- 307: redirect to leader (cluster mode, follower received the request)
- 409: consolidation already in flight for this session
- 422: session has fewer messages than the target window; nothing to consolidate
- 500: summarization or replication failed

**example:**
```sh
curl -X POST http://localhost:3000/sessions/{session_id}/consolidate
```
