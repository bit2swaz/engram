#!/bin/bash
set -e

ENGRAM_HOST_PORT="${ENGRAM_HOST_PORT:-3002}"

echo "Waiting for engram to become healthy..."
for i in {1..30}; do
    if curl -sf "http://localhost:${ENGRAM_HOST_PORT}/health" > /dev/null; then
        echo "enGRAM is healthy!"
        exit 0
    fi
    sleep 1
done

echo "Health check failed after 30s"
exit 1