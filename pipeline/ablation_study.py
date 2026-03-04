"""
Feature Ablation Study for Valuation Models
============================================
Loads the cached listings CSV + trained best_model.pkl for each segment,
then re-evaluates MAPE after dropping feature groups one at a time.

Ablation groups:
  A  "district"                     → location signal
  B  "property_age"                 → temporal depreciation signal
  C  interaction features           → beds_sqft, beds_sq, log_beds_sqft, sqft_bin, log_sqft
  D  all engineered features (B+C)  → keep only raw beds, sqft, is_freehold, district

Output: models/valuation/ablation_results.json  +  console table

Usage:
    python pipeline/ablation_study.py
"""

import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

warnings.filterwarnings("ignore")

# ── paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR   = PROJECT_ROOT / "models" / "valuation"
CACHE_CSV    = OUTPUT_DIR / "listings_cache.csv"

# ── constants (must match valuation_model.py) ────────────────────────────────
CURRENT_YEAR  = 2026
RANDOM_STATE  = 42
TEST_SIZE     = 0.20
SALE_PRICE_MIN, SALE_PRICE_MAX = 100_000, 200_000_000
RENT_PRICE_MIN, RENT_PRICE_MAX = 100,     150_000

SEGMENTS = [
    ("Condominium",        "condo", "sale"),
    ("Condominium",        "condo", "rent"),
    ("HDB",                "hdb",   "sale"),
    ("HDB",                "hdb",   "rent"),
    ("Landed",             "landed","sale"),
    ("Landed",             "landed","rent"),
    ("Good Class Bungalow","gcb",   "sale"),
    ("Good Class Bungalow","gcb",   "rent"),
]

ALL_FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold",
    "property_age", "district",
]

ABLATION_GROUPS = {
    "Full model (baseline)":          ALL_FEATURES,
    "− district":                     [f for f in ALL_FEATURES if f != "district"],
    "− property_age":                 [f for f in ALL_FEATURES if f != "property_age"],
    "− interaction features":         ["beds", "sqft", "is_freehold", "property_age", "district"],
    "− property_age & interactions":  ["beds", "sqft", "is_freehold", "district"],
}


# ── helpers ──────────────────────────────────────────────────────────────────
def mape(y_true_log, y_pred_log):
    yt = 10 ** y_true_log
    yp = 10 ** y_pred_log
    return float(np.mean(np.abs((yt - yp) / yt)) * 100)


def _parse_year(val):
    if pd.isna(val):
        return np.nan
    digits = "".join(c for c in str(val) if c.isdigit())
    if len(digits) >= 4:
        yr = int(digits[:4])
        if 1960 <= yr <= CURRENT_YEAR + 5:
            return CURRENT_YEAR - yr
    return np.nan


def prepare_segment(df_raw: pd.DataFrame, property_type: str, mode: str) -> pd.DataFrame:
    br        = "property-for-sale" if mode == "sale" else "property-for-rent"
    price_min = SALE_PRICE_MIN if mode == "sale" else RENT_PRICE_MIN
    price_max = SALE_PRICE_MAX if mode == "sale" else RENT_PRICE_MAX

    df = df_raw[
        (df_raw["property_type"] == property_type) &
        (df_raw["buy_rent"] == br) &
        (df_raw["price"].between(price_min, price_max))
    ].copy()

    df["beds"] = pd.to_numeric(df["beds"], errors="coerce")
    df["beds"] = df["beds"].fillna(df["beds"].median()).clip(0, 20)
    df = df[df["beds"] >= 1]

    df["sqft"] = pd.to_numeric(df["sqft"], errors="coerce")
    df = df[df["sqft"].between(50, 50_000)]

    df["is_freehold"]   = df["tenure"].fillna("").str.lower().str.contains("freehold").astype(int)
    df["log_sqft"]      = np.log10(df["sqft"].clip(1))
    df["log_price"]     = np.log10(df["price"])
    df["beds_sqft"]     = df["beds"] * df["sqft"]
    df["beds_sq"]       = df["beds"] ** 2
    df["log_beds_sqft"] = df["beds"] * df["log_sqft"]
    df["sqft_bin"]      = pd.qcut(df["sqft"], q=5, labels=False, duplicates="drop")
    df["property_age"]  = df["built_year"].apply(_parse_year)
    med_age = df["property_age"].median()
    df["property_age"]  = df["property_age"].fillna(med_age if not np.isnan(med_age) else 10)
    df["district"]      = pd.to_numeric(df["district"], errors="coerce")
    med_dist = df["district"].median()
    df["district"]      = df["district"].fillna(med_dist if not np.isnan(med_dist) else 15)

    return df


# ── main ─────────────────────────────────────────────────────────────────────
def run_ablation():
    print(f"\nLoading cache: {CACHE_CSV}")
    df_raw = pd.read_csv(CACHE_CSV)
    print(f"  {len(df_raw):,} rows loaded.\n")

    all_results = {}

    # Header
    col_w = 38
    header = f"{'Segment':<18}" + "".join(f"{k:<{col_w}}" for k in ABLATION_GROUPS)
    print(header)
    print("-" * len(header))

    for property_type, type_key, mode in SEGMENTS:
        seg_label  = f"{type_key}_{mode}"
        model_path = OUTPUT_DIR / seg_label / "best_model.pkl"

        if not model_path.exists():
            print(f"  {seg_label:<18} [model not found — skip]")
            continue

        model = joblib.load(model_path)

        df_seg = prepare_segment(df_raw, property_type, mode)
        if len(df_seg) < 50:
            print(f"  {seg_label:<18} [too few samples ({len(df_seg)}) — skip]")
            continue

        _, X_test_full, _, y_test = train_test_split(
            df_seg[ALL_FEATURES], df_seg["log_price"],
            test_size=TEST_SIZE, random_state=RANDOM_STATE
        )

        seg_results = {}
        row_str = f"{seg_label:<18}"

        for group_name, features in ABLATION_GROUPS.items():
            # Replace dropped features with zeros in the test set
            X_test_abl = X_test_full.copy()
            dropped = [f for f in ALL_FEATURES if f not in features]
            for f in dropped:
                X_test_abl[f] = 0.0   # zero-out: equivalent to removing signal

            y_pred = model.predict(X_test_abl[ALL_FEATURES])
            m = mape(y_test.values, y_pred)
            seg_results[group_name] = round(m, 2)

            full_mape = seg_results.get("Full model (baseline)", m)
            delta = m - full_mape
            delta_str = f"(+{delta:+.2f}pp)" if group_name != "Full model (baseline)" else ""
            cell = f"{m:.2f}% {delta_str}"
            row_str += f"{cell:<{col_w}}"

        all_results[seg_label] = seg_results
        print(row_str)

    # Save JSON
    out_path = OUTPUT_DIR / "ablation_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved: {out_path}")

    # ── Pretty summary table (MAPE delta vs full model) ──────────────────────
    print("\n\n=== MAPE DELTA vs Full Model (pp = percentage points) ===")
    group_names = list(ABLATION_GROUPS.keys())[1:]   # skip "Full model" itself
    print(f"{'Segment':<18}" + "".join(f"{g[:28]:<30}" for g in group_names))
    print("-" * (18 + 30 * len(group_names)))

    for seg_label, res in all_results.items():
        full = res.get("Full model (baseline)", None)
        if full is None:
            continue
        row = f"{seg_label:<18}"
        for g in group_names:
            delta = res.get(g, full) - full
            row += f"{delta:+.2f}pp{'':<24}"
        print(row)

    return all_results


if __name__ == "__main__":
    run_ablation()
