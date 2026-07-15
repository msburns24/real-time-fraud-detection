"""
Fraud Detection Logic (PROVIDED baseline; extend if you like).

Loads the trained model artifact when present. If it is missing, it falls back
to a transparent rule-based scorer so the API still runs end-to-end out of the
box (this IS the "baseline model" referenced in the assignment). Running
`python -m src.models.train` upgrades it to the trained scikit-learn model.
"""
from __future__ import annotations

import os

try:
    import joblib
except Exception:  # joblib optional at import time
    joblib = None


class FraudDetector:
    def __init__(self, model_path: str | None = None):
        self.model_path = model_path or os.getenv("MODEL_PATH", "models/fraud_model_v1.pkl")
        self.model = None
        self.model_version = "rule-based-fallback"
        self._load()

    def _load(self) -> None:
        if joblib is not None and os.path.exists(self.model_path):
            try:
                bundle = joblib.load(self.model_path)
                self.model = bundle["model"]
                self.feature_order = bundle["feature_order"]
                self.model_version = bundle.get("version", "sklearn-v1")
            except Exception as exc:
                print(f"[detector] could not load model ({exc}); using fallback", flush=True)

    def predict(self, features: dict) -> dict:
        """features: merged transaction + customer features. Returns a dict with
        fraud_probability (0..1), is_fraud (0/1), and model_version."""
        if self.model is not None:
            row = [[float(features.get(name, 0.0)) for name in self.feature_order]]
            prob = float(self.model.predict_proba(row)[0][1])
        else:
            prob = self._rule_score(features)
        return {
            "fraud_probability": round(prob, 4),
            "is_fraud": int(prob >= 0.5),
            "model_version": self.model_version,
        }

    @staticmethod
    def _rule_score(f: dict) -> float:
        """Simple, explainable baseline: score rises with how far this amount is
        above the customer's recent average, plus an online bump."""
        amount = float(f.get("amount", 0.0))
        avg = float(f.get("avg_amount", 0.0)) or 1.0
        ratio = amount / avg
        score = 0.0
        if ratio > 5:
            score += 0.6
        elif ratio > 3:
            score += 0.35
        elif ratio > 2:
            score += 0.15
        if f.get("is_online"):
            score += 0.1
        if float(f.get("transaction_count", 0)) > 20:
            score += 0.1
        return max(0.0, min(1.0, score))
