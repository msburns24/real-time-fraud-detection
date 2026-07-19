#!/usr/bin/env bash
# Blue-green cutover demo with per-colour accounting.
#
# Runs continuous load against the stable :8080 endpoint, fires
# switch_traffic.sh at a fixed offset, then reports how many requests each
# colour actually served.
#
# The per-colour counts are the point. An error count alone cannot prove a
# cutover happened: a switch that fires after the load finishes yields a
# perfect `errors: 0` while nothing moved. Only both colours showing non-zero
# traffic distinguishes a real mid-load switch.
#
# Usage:  bash scripts/bg_demo.sh [duration_seconds] [switch_at_seconds]
set -euo pipefail

DURATION="${1:-20}"
SWITCH_AT="${2:-8}"
BG="docker compose -f deployment/docker-compose.blue-green.yml"
URL="http://localhost:8080/predict"
PAYLOAD='{"transaction_id":"bg","customer_id":"CUST0001","amount":250.0,
          "merchant_category":"online_retail","is_online":true,
          "timestamp":"2026-01-01T00:50:00Z"}'

blue=$($BG ps -q api-blue)
green=$($BG ps -q api-green)

# Baseline the access-log line counts so we measure only this run.
count() { docker logs "$1" 2>&1 | grep -c 'POST /predict' || true; }
blue0=$(count "$blue"); green0=$(count "$green")

echo "Active colour before: $(grep -qE '^[[:space:]]*server api-blue:8000;' \
  deployment/nginx/nginx.conf && echo blue || echo green)"
echo "Load: ${DURATION}s against :8080, switching at ${SWITCH_AT}s"
echo

# Continuous load in the background.
errors=0
( end=$((SECONDS + DURATION))
  while [ $SECONDS -lt $end ]; do
    curl -s -o /dev/null -w '%{http_code}\n' -H 'content-type: application/json' \
      -d "$PAYLOAD" "$URL"
  done > /tmp/bg_codes.txt ) &
load_pid=$!

sleep "$SWITCH_AT"
echo "--- firing switch_traffic.sh ---"
bash deployment/switch_traffic.sh
echo "--- switch returned, load continues ---"
echo

wait $load_pid

total=$(wc -l < /tmp/bg_codes.txt)
ok=$(grep -c '^200$' /tmp/bg_codes.txt || true)
errors=$((total - ok))
blue1=$(count "$blue"); green1=$(count "$green")

echo "Active colour after:  $(grep -qE '^[[:space:]]*server api-blue:8000;' \
  deployment/nginx/nginx.conf && echo blue || echo green)"
echo
printf 'requests sent      : %s\n' "$total"
printf 'non-200 responses  : %s\n' "$errors"
printf 'served by blue     : %s\n' "$((blue1 - blue0))"
printf 'served by green    : %s\n' "$((green1 - green0))"
echo
if [ "$((blue1 - blue0))" -gt 0 ] && [ "$((green1 - green0))" -gt 0 ]; then
  echo "PASS: both colours served traffic — the cutover happened mid-load."
else
  echo "INCONCLUSIVE: only one colour served traffic. The switch did not land"
  echo "inside the load window, so this run proves nothing about downtime."
fi
