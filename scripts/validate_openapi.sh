#!/bin/bash
set -e

API_URL="http://localhost:3000/api-docs/openapi.json"
HEALTH_URL="http://localhost:3000/health"
REQUIRED_PATHS=(
  "/sessions"
  "/sessions/{session_id}/messages"
  "/sessions/{session_id}/context"
  "/sessions/{session_id}/search"
  "/sessions/{session_id}"
  "/sessions/{session_id}/core-memory"
  "/health"
  "/metrics"
)

# check for jq
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is not installed. please install jq to continue."
  exit 1
fi

# check if server is running
if ! curl -sf "$HEALTH_URL" >/dev/null; then
  echo "server is not running. please start it with 'cargo run &' or 'docker compose up' and try again."
  exit 1
fi

echo "fetching openapi spec from $API_URL..."
OPENAPI_JSON=$(curl -sf "$API_URL")

# check openapi version
OPENAPI_VERSION=$(echo "$OPENAPI_JSON" | jq -r '.openapi // empty')
if [ -z "$OPENAPI_VERSION" ]; then
  echo "error: openapi version field is missing."
  exit 1
fi

echo "openapi version: $OPENAPI_VERSION"

# check required paths
MISSING=0
for path in "${REQUIRED_PATHS[@]}"; do
  if ! echo "$OPENAPI_JSON" | jq -e --arg p "$path" '.paths | has($p)' >/dev/null; then
    echo "error: missing path in openapi spec: $path"
    MISSING=1
  fi
done

if [ "$MISSING" -eq 1 ]; then
  echo "one or more required paths are missing. please fix #[utoipa::path] annotations in src/server.rs and re-run."
  exit 1
fi

echo "all required paths are present in the openapi spec. validation successful."
exit 0
