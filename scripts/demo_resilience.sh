#!/usr/bin/env bash
# Graceful-degradation demo. Kill the feature store mid-flight: /predict keeps
# returning 200, but scored from the transaction alone.
#
# The $130 purchase is the one worth watching. With CUST0001's streamed history
# (avg ~$125) it scores 0.0 — plainly normal. With Redis down there is no
# history to compare against, and the same request scores 1.0: a false positive.
# That is the honest cost of degrading — availability is preserved, accuracy is
# not.
set -euo pipefail

PAYLOAD='{"transaction_id":"r1","customer_id":"CUST0001","amount":130,
          "merchant_category":"grocery","is_online":false,
          "timestamp":"2026-01-01T00:50:00Z"}'

score() {
  curl -s -o /tmp/resil_body.txt -w 'HTTP %{http_code}' localhost:8000/predict \
    -H 'content-type: application/json' -d "$PAYLOAD"
  printf '   '
  python3 scripts/_fmt_prediction.py < /tmp/resil_body.txt
}

echo "A routine \$130 grocery purchase by CUST0001 (recent average ~\$125)."
echo
echo "Redis up — scored against streamed history:"
score
echo
echo "--- docker compose stop redis ---"
docker compose stop redis >/dev/null 2>&1
echo
echo "Redis down — identical request:"
score
echo
echo "still 200, but no history to compare against: a false positive."
echo "the API says why, as structured data:"
docker compose logs api --tail 60 2>/dev/null \
  | grep -i 'feature lookup degraded' | tail -1 \
  | sed 's/^api-1  *| //' | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-120
echo
echo "--- docker compose start redis ---"
docker compose start redis >/dev/null 2>&1
sleep 2
echo "recovered, no restart of the API required:"
score
