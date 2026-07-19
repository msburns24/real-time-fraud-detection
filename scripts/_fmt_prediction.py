"""Format a /predict response as one readable line (screencast helper)."""

import json
import sys

d = json.load(sys.stdin)
print(
    f"  fraud_probability={d['fraud_probability']}"
    f"  is_fraud={d['is_fraud']}"
    f"  model={d['model_version']}"
    f"  latency={d['latency_ms']}ms"
)
