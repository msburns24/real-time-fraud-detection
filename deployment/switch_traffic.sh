#!/usr/bin/env bash
# Blue-green traffic switch (no Kubernetes). Flips nginx between api-blue and
# api-green with a zero-downtime reload, after verifying the target is healthy.
# Run again to roll back. Run from the repo root.
set -euo pipefail

COMPOSE="docker compose -f deployment/docker-compose.blue-green.yml"
CONF="deployment/nginx/nginx.conf"

if grep -qE '^[[:space:]]*server api-blue:8000;' "$CONF"; then
  current=blue; target=green; target_port=8002
else
  current=green; target=blue; target_port=8001
fi
echo "Active version: $current  ->  switching to: $target"

# 1. Verify the target version is healthy before cutting over
ok=0
for _ in $(seq 1 20); do
  if curl -sf "http://localhost:${target_port}/health" >/dev/null 2>&1; then ok=1; break; fi
  sleep 2
done
if [ "$ok" -ne 1 ]; then
  echo "ERROR: $target is not healthy on :${target_port}. Staying on $current."
  exit 1
fi

# 2. Flip the active/standby server lines in nginx.conf
if [ "$target" = "green" ]; then
  sed -i.bak -E \
    -e 's|^([[:space:]]*)server api-blue:8000;|\1# server api-blue:8000;|' \
    -e 's|^([[:space:]]*)# server api-green:8000;|\1server api-green:8000;|' "$CONF"
else
  sed -i.bak -E \
    -e 's|^([[:space:]]*)server api-green:8000;|\1# server api-green:8000;|' \
    -e 's|^([[:space:]]*)# server api-blue:8000;|\1server api-blue:8000;|' "$CONF"
fi

# 3. Reload nginx (drains in-flight requests, no dropped connections)
$COMPOSE exec -T nginx nginx -s reload
echo "Switched to $target. Endpoint http://localhost:8080 is now serving $target."
echo "Rollback: run this script again."
