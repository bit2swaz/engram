# API Reference

This document describes every REST endpoint exposed by engram. All endpoints are served at `http://localhost:3000` by default.

| method | path                                   | description                                 |
|--------|----------------------------------------|---------------------------------------------|
| POST   | /sessions                             | create a new session                        |
| POST   | /sessions/{session_id}/messages        | add a message                               |
| GET    | /sessions/{session_id}/context         | get assembled context                       |
| POST   | /sessions/{session_id}/search          | semantic search over long-term memory       |
| DELETE | /sessions/{session_id}                 | delete a session and all its memories       |
| PUT    | /sessions/{session_id}/core-memory     | add a core memory fact                      |
| GET    | /health                               | health check                                |
| GET    | /metrics                              | Prometheus metrics                          |
| GET    | /api-docs/openapi.json                 | OpenAPI specification                       |
| GET    | /swagger-ui/                          | Swagger UI                                  |
| GET    | /cluster                              | cluster status (cluster mode only)          |
| POST   | /cluster/init                         | initialize the Raft cluster                 |
| POST   | /cluster/add-learner                  | add a learner node                          |
| POST   | /cluster/change-membership            | promote learners to full voting members     |

---

## POST /sessions

create a new session.

**request body:** none

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

**example:**
```sh
curl -X POST http://localhost:3000/sessions
```

---

## POST /sessions/{session_id}/messages

add a message to a session.

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

get the assembled context for a session.

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

semantic search over long-term memory.

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

delete a session and all its memories.

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

add a core memory fact to a session.

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

## GET /health

health check endpoint.

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

these endpoints are only available when the node is started in cluster mode (i.e., `NODE_ID` is set). standalone nodes return 503.

---

## GET /cluster

returns the current Raft cluster status for this node.

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

initializes the Raft cluster. call this once from any node after all nodes are running. reads `NODE_ID`, `RAFT_ADDR` (or `RAFT_ADVERTISE_ADDR`), and `CLUSTER_PEERS` from the node's environment to build the initial membership set.

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

adds a new node as a learner. learners receive log replication but do not vote in elections. promote to a full member with `/cluster/change-membership`.

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

changes the cluster membership to the given set of node IDs. nodes in the new set that are currently learners are promoted to full members; nodes not in the new set are removed.

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
