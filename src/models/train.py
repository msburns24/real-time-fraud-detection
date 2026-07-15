"""
Baseline model training (PROVIDED).

Trains a simple, honest fraud classifier on data/seed_transactions.csv and saves
it to models/fraud_model_v1.pkl. Model accuracy is NOT graded (this is a systems
course) — the point is a working artifact the serving path can load.

Feature order MUST match what the API sends at inference time
(see src/api/fraud_detector.py):  ["amount", "is_online", "avg_amount", "transaction_count"]

Run:  python -m src.models.train      (after: python data/generate_seed.py)
"""
from __future__ import annotations

import os

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression

FEATURE_ORDER = ["amount", "is_online", "avg_amount", "transaction_count"]


def main() -> None:
    here = os.path.dirname(__file__)
    csv_path = os.path.join(here, "..", "..", "data", "seed_transactions.csv")
    df = pd.read_csv(csv_path)

    # per-customer aggregates → the same features the store serves at inference
    agg = df.groupby("customer_id")["amount"].agg(avg_amount="mean", transaction_count="count")
    df = df.join(agg, on="customer_id")
    df["is_online"] = df["is_online"].astype(int)

    X = df[FEATURE_ORDER].astype(float)
    y = df["is_fraud"].astype(int)

    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(X, y)

    out_dir = os.path.join(here, "..", "..", "models")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "fraud_model_v1.pkl")
    joblib.dump({"model": model, "feature_order": FEATURE_ORDER, "version": "sklearn-logreg-v1"}, out_path)
    print(f"saved model -> {out_path}  (train fraud rate={y.mean():.3f})")


if __name__ == "__main__":
    main()
