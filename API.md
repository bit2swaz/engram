# api reference

this document describes every rest endpoint exposed by engram. all endpoints are served at `http://localhost:3000` by default.

| method | path                                   | description                                 |
|--------|----------------------------------------|---------------------------------------------|
| post   | /sessions                             | create a new session                        |
| post   | /sessions/{session_id}/messages        | add a message                               |
| get    | /sessions/{session_id}/context         | get assembled context                       |
| post   | /sessions/{session_id}/search          | semantic search over long-term memory       |
| delete | /sessions/{session_id}                 | delete a session and all its memories       |
| put    | /sessions/{session_id}/core-memory     | add a core memory fact                      |
| get    | /health                               | health check                                |
| get    | /metrics                              | prometheus metrics                          |
| get    | /api-docs/openapi.json                 | openapi specification                       |
| get    | /swagger-ui                           | swagger ui                                  |

---

## post /sessions

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

## post /sessions/{session_id}/messages

add a message to a session.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "id": "optional-client-generated-uuid", // optional
  "role": "user",                        // required, "user" | "assistant" | "system"
  "content": "hello, what is rust?"       // required
}
```

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

## get /sessions/{session_id}/context

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

## post /sessions/{session_id}/search

semantic search over long-term memory.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "query": "rust async",   // required
  "top_k": 5               // required
}
```

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

## delete /sessions/{session_id}

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

## put /sessions/{session_id}/core-memory

add a core memory fact to a session.

**path parameters:**
- `session_id` (string): session identifier

**request body:**
```json
{
  "fact": "user prefers dark mode" // required
}
```

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

## get /health

health check endpoint.

**success response:**
- status: 200

**example:**
```sh
curl http://localhost:3000/health
```

---

## get /metrics

prometheus metrics endpoint.

**success response:**
- status: 200
- body: prometheus text format

**example:**
```sh
curl http://localhost:3000/metrics
```

---

## get /api-docs/openapi.json

openapi specification (json).

**success response:**
- status: 200
- body: openapi json

**example:**
```sh
curl http://localhost:3000/api-docs/openapi.json | jq
```

---

## get /swagger-ui

swagger ui for interactive api docs.

**success response:**
- status: 200
- body: html

**example:**
```sh
curl http://localhost:3000/swagger-ui
```
