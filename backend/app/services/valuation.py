"""
ValuationService
================
Loads per-(property_type × mode) models from models/valuation/{type_key}_{mode}/
and serves estimates with SHAP attribution.

Model directory layout:
  models/valuation/
    condo_sale/   best_model.pkl  metrics.json
    condo_rent/   best_model.pkl  metrics.json
    hdb_sale/     best_model.pkl  metrics.json
    hdb_rent/     best_model.pkl  metrics.json
    landed_sale/  best_model.pkl  metrics.json
    landed_rent/  best_model.pkl  metrics.json
    gcb_sale/     best_model.pkl  metrics.json
    gcb_rent/     best_model.pkl  metrics.json
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import joblib

# ─── paths ────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent   # services→app→backend→project
_MODEL_DIR    = _PROJECT_ROOT / "models" / "valuation"

# Canonical → dir-key mapping (must match pipeline/valuation_model.py)
_TYPE_KEYS = {
    "condominium":         "condo",
    "condo":               "condo",
    "hdb":                 "hdb",
    "landed":              "landed",
    "good class bungalow": "gcb",
    "gcb":                 "gcb",
}

# Features (no property_type — each model is already pure)
NUMERIC_FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold",
    "property_age",   # 2026 - built_year (99.8% coverage in DB)
]

_CURRENT_YEAR = 2026

# sqft quantile bin boundaries (approximate, per mode)
_SALE_BINS = [0, 600, 900, 1300, 2200, 1e9]
_RENT_BINS = [0, 350, 520, 750, 1200, 1e9]

# Default MAPE fallback if metrics.json missing
_DEFAULT_MAPE = 0.25

# ─── model + metrics cache ────────────────────────────────────────────────────
_models:  dict = {}
_metrics: dict = {}


def _type_key(property_type: str) -> str:
    return _TYPE_KEYS.get(property_type.lower().strip(), "condo")


def _mode_key(buy_rent: str) -> str:
    return "rent" if "rent" in buy_rent.lower() else "sale"


def _seg_key(property_type: str, buy_rent: str) -> str:
    return f"{_type_key(property_type)}_{_mode_key(buy_rent)}"


def _load_model(seg: str):
    if seg in _models:
        return _models[seg]
    path = _MODEL_DIR / seg / "best_model.pkl"
    if not path.exists():
        raise FileNotFoundError(
            f"Model not found: {path}. "
            "Run: python pipeline/valuation_model.py"
        )
    _models[seg]  = joblib.load(path)
    # Load metrics
    metrics_path = _MODEL_DIR / seg / "metrics.json"
    if metrics_path.exists():
        _metrics[seg] = json.loads(metrics_path.read_text())
    return _models[seg]


def _get_mape(seg: str) -> float:
    m = _metrics.get(seg, {})
    return m.get("MAPE_pct", _DEFAULT_MAPE * 100) / 100   # stored as 22.1 → return 0.221


# ─── feature engineering ──────────────────────────────────────────────────────
def _build_features(
    beds: float, sqft: float, tenure: Optional[str], mode: str,
    built_year: Optional[int] = None,
) -> pd.DataFrame:
    is_freehold  = 1 if tenure and "freehold" in tenure.lower() else 0
    log_sqft     = np.log10(max(sqft, 1))
    bins         = _SALE_BINS if mode == "sale" else _RENT_BINS
    sqft_bin     = int(np.digitize(sqft, bins) - 1)
    property_age = (_CURRENT_YEAR - built_year) if built_year and 1960 <= built_year <= _CURRENT_YEAR + 5 else 10

    return pd.DataFrame([{
        "beds":          beds,
        "sqft":          sqft,
        "log_sqft":      log_sqft,
        "beds_sqft":     beds * sqft,
        "beds_sq":       beds ** 2,
        "log_beds_sqft": beds * log_sqft,
        "sqft_bin":      sqft_bin,
        "is_freehold":   is_freehold,
        "property_age":  property_age,
    }])


# ─── SHAP attribution ─────────────────────────────────────────────────────────
_FEATURE_LABELS = {
    "beds":          "Bedrooms",
    "sqft":          "Floor area (sqft)",
    "log_sqft":      "Floor area (log)",
    "beds_sqft":     "Beds × sqft",
    "beds_sq":       "Bedrooms²",
    "log_beds_sqft": "Beds × log(sqft)",
    "sqft_bin":      "Size tier",
    "is_freehold":   "Freehold tenure",
    "property_age":  "Property age (yrs)",
}


def _get_shap_factors(model, X_df: pd.DataFrame, mode: str) -> list[dict]:
    try:
        import shap

        pre        = model.named_steps["pre"]
        model_step = model.named_steps["m"]
        X_t        = pre.transform(X_df)
        X_tdf      = pd.DataFrame(X_t, columns=NUMERIC_FEATURES)

        model_name = type(model_step).__name__
        if any(k in model_name for k in ("XGB", "LGBM", "Forest", "Gradient")):
            explainer = shap.TreeExplainer(model_step)
        else:
            explainer = shap.LinearExplainer(model_step, X_tdf)

        shap_vals = explainer.shap_values(X_tdf)
        vals      = np.array(shap_vals).flatten()

        try:
            base_log = float(explainer.expected_value)
        except Exception:
            base_log = float(np.log10(800_000))

        base_price = 10 ** base_log
        factors = []
        for feat, sv in zip(NUMERIC_FEATURES, vals):
            impact = (10 ** (base_log + sv)) - base_price
            factors.append({
                "feature":    feat,
                "label":      _FEATURE_LABELS.get(feat, feat),
                "shap_value": round(float(sv), 4),
                "impact_sgd": int(impact),
                "direction":  "positive" if sv > 0 else "negative",
            })

        factors.sort(key=lambda x: abs(x["impact_sgd"]), reverse=True)
        return factors[:5]

    except Exception as e:
        return [{"label": "Attribution unavailable", "error": str(e)}]


# ─── main API ─────────────────────────────────────────────────────────────────
def estimate(
    property_type: str,
    buy_rent:      str,
    beds:          float,
    sqft:          float,
    tenure:        Optional[str] = None,
    actual_price:  Optional[float] = None,
    built_year:    Optional[int] = None,
) -> dict:
    mode  = _mode_key(buy_rent)
    seg   = _seg_key(property_type, buy_rent)
    model = _load_model(seg)
    mape  = _get_mape(seg)

    X_df    = _build_features(beds, sqft, tenure, mode, built_year=built_year)
    log_est = float(model.predict(X_df)[0])
    est     = round(10 ** log_est)

    range_low  = round(est * (1 - mape))
    range_high = round(est * (1 + mape))

    verdict = premium_pct = None
    if actual_price and actual_price > 0:
        premium_pct = round((actual_price - est) / est * 100, 1)
        verdict = (
            "overpriced"      if premium_pct >  15 else
            "below_estimate"  if premium_pct < -15 else
            "fair_value"
        )

    shap_factors = _get_shap_factors(model, X_df, mode)

    return {
        "estimate":      est,
        "range_low":     range_low,
        "range_high":    range_high,
        "mode":          mode,
        "segment":       seg,
        "mape":          mape,
        "premium_pct":   premium_pct,
        "verdict":       verdict,
        "shap_factors":  shap_factors,
        "disclaimer": (
            "Estimate based on property characteristics only (size, tenure). "
            "Location premium not factored in due to limited geographic data."
        ),
    }


def health() -> dict:
    result = {}
    for seg_key in ["condo_sale", "condo_rent", "hdb_sale", "hdb_rent",
                    "landed_sale", "landed_rent", "gcb_sale", "gcb_rent"]:
        path = _MODEL_DIR / seg_key / "best_model.pkl"
        result[seg_key] = {
            "available": path.exists(),
            "loaded":    seg_key in _models,
            "mape_pct":  _metrics.get(seg_key, {}).get("MAPE_pct"),
        }
    return result
