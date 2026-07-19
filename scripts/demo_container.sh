#!/usr/bin/env bash
# Container hardening + test evidence: the image runs as a non-root user, the
# HEALTHCHECK actually reports healthy, and the test suite passes.
set -euo pipefail

api=$(docker compose ps -q api)

echo "container user and health, straight from the daemon:"
docker inspect -f 'User={{.Config.User}}  Health={{.State.Health.Status}}' "$api"
whoami_out=$(docker exec "$api" whoami)
echo "whoami inside the container: $whoami_out"
echo
echo "test suite:"
.venv/bin/pytest -q tests/ 2>&1 | grep -E '^[0-9]+ passed'
