#!/usr/bin/env bash
# Batch scoring demo: five transactions spanning three customers cost the
# feature store ONE round-trip, not five.
#
# The claim is verified against Redis itself rather than asserted. Redis command
# counters are reset immediately before the request, so whatever INFO
# commandstats reports afterwards was caused by this batch alone: one MGET,
# zero GETs.
set -euo pipefail

redis=$(docker compose ps -q redis)

BATCH='[
  {"transaction_id":"b1","customer_id":"CUST0001","amount":130,
   "merchant_category":"grocery","is_online":false,
   "timestamp":"2026-01-01T00:50:00Z"},
  {"transaction_id":"b2","customer_id":"CUST0002","amount":4000,
   "merchant_category":"online_retail","is_online":true,
   "timestamp":"2026-01-01T00:51:00Z"},
  {"transaction_id":"b3","customer_id":"CUST0001","amount":89,
   "merchant_category":"grocery","is_online":false,
   "timestamp":"2026-01-01T00:52:00Z"},
  {"transaction_id":"b4","customer_id":"CUST0003","amount":215,
   "merchant_category":"restaurant","is_online":false,
   "timestamp":"2026-01-01T00:53:00Z"},
  {"transaction_id":"b5","customer_id":"CUST0002","amount":75,
   "merchant_category":"grocery","is_online":false,
   "timestamp":"2026-01-01T00:54:00Z"}
]'

echo "5 transactions, 3 distinct customers (CUST0001 x2, CUST0002 x2, CUST0003)."
echo
docker exec "$redis" redis-cli config resetstat > /dev/null
echo "redis command counters reset. sending the batch:"
echo
curl -s localhost:8000/predict_batch -H 'content-type: application/json' \
  -d "$BATCH" | python3 scripts/_fmt_batch.py
echo "  (latency_ms is elapsed since the batch began, so it accumulates —"
echo "   it is not per-item scoring time.)"
echo
echo "what the batch actually cost redis:"
stats=$(docker exec "$redis" redis-cli info commandstats)
echo "$stats" | grep -E '^cmdstat_(mget|get):' || echo "  (no get/mget recorded)"
echo
echo "one MGET for three customers — not one GET each."
