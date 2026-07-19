#!/usr/bin/env bash
# Scoring demo: the same customer, two very different transactions, showing the
# prediction genuinely consumes streamed features rather than echoing the
# amount. Then the per-request structured latency log.
set -euo pipefail

post() {
  curl -s localhost:8000/predict -H 'content-type: application/json' -d "$1" \
    | python3 scripts/_fmt_prediction.py
}

echo "CUST0001 recent average is ~\$125, streamed from Kafka:"
docker exec "$(docker compose ps -q redis)" redis-cli get features:CUST0001
echo
echo "1) a typical \$130 purchase"
post '{"transaction_id":"d1","customer_id":"CUST0001","amount":130,
       "merchant_category":"grocery","is_online":false,
       "timestamp":"2026-01-01T00:50:00Z"}'
echo
echo "2) same customer, \$4000 online"
post '{"transaction_id":"d2","customer_id":"CUST0001","amount":4000,
       "merchant_category":"online_retail","is_online":true,
       "timestamp":"2026-01-01T00:50:00Z"}'
echo
echo "per-request structured log from the API container:"
docker compose logs api --tail 40 2>/dev/null \
  | grep 'prediction served' | tail -1 | sed 's/^api-1  *| //' | cut -c1-140
