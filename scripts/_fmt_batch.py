"""Format a /predict_batch response as an aligned table for the screencast.

Kept out of the tape (and out of an inline `python -c`) because VHS's parser
rejects embedded quotes, and nesting quotes through bash into python is how the
single-transaction formatter broke.
"""

import json
import sys

rows = json.load(sys.stdin)
print(f"  {'txn':<5} {'probability':>12}  {'is_fraud':>8}  {'latency_ms':>10}")
for r in rows:
    print(
        f"  {r['transaction_id']:<5} {r['fraud_probability']:>12.3f}"
        f"  {r['is_fraud']:>8}  {r['latency_ms']:>10.3f}"
    )
print(f"\n  {len(rows)} predictions, returned in request order.")
