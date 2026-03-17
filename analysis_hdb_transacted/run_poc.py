"""
POC: HDB Resale Transacted Price Model — time-aligned comparison
Reads local CSV downloaded from data.gov.sg — no API calls.

Methodology:
  - Only 2024–2026 transactions (same period as listing data snapshot)
  - Identical feature set to main pipeline valuation_model.py
  - Same XGBoost hyperparameters, same 80/20 split, same CV folds
  - Comparison metric: MAPE on held-out test set + 5-fold CV R²

Run:
    cd /Users/alanwang/PycharmProjects/PythonProject
    python poc_hdb_transacted/run_poc.py
"""
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
CURRENT_YEAR  = 2026
RANDOM_STATE  = 42
TEST_SIZE     = 0.20
CV_FOLDS      = 5
# Only use transactions from 2024 onwards — same period as listing snapshot
YEAR_CUTOFF   = 2024

POC_DIR  = Path(__file__).parent
CSV_PATH = POC_DIR / "results" / "ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv"
OUT_JSON = POC_DIR / "results" / "poc_results.json"

TOWN_TO_DISTRICT = {
    "ANG MO KIO": 20, "BEDOK": 16, "BISHAN": 20, "BUKIT BATOK": 23,
    "BUKIT MERAH": 3,  "BUKIT PANJANG": 23, "BUKIT TIMAH": 21,
    "CENTRAL AREA": 1, "CHOA CHU KANG": 23, "CLEMENTI": 5,
    "GEYLANG": 14, "HOUGANG": 19, "JURONG EAST": 22, "JURONG WEST": 22,
    "KALLANG/WHAMPOA": 12, "MARINE PARADE": 15, "PASIR RIS": 18,
    "PUNGGOL": 19, "QUEENSTOWN": 3, "SEMBAWANG": 27, "SENGKANG": 19,
    "SERANGOON": 19, "TAMPINES": 18, "TOA PAYOH": 12, "WOODLANDS": 25,
    "YISHUN": 27, "LIM CHU KANG": 25, "TENGAH": 22,
}

def mape(y_true, y_pred):
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100

# ── Load ──────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  POC: HDB Transacted Price Model")
print("=" * 60)
print(f"\n[1] Loading {CSV_PATH.name} ...")
df = pd.read_csv(CSV_PATH)
print(f"    {len(df):,} rows loaded")
print(f"    columns: {list(df.columns)}")

# ── Feature engineering ───────────────────────────────────────────────────────
print("\n[2] Feature engineering ...")

df["resale_price"]   = pd.to_numeric(df["resale_price"],   errors="coerce")
df["floor_area_sqm"] = pd.to_numeric(df["floor_area_sqm"], errors="coerce")
df["sqft"]           = df["floor_area_sqm"] * 10.764

# Beds: SG convention — "4 ROOM" = 3 bedrooms
flat_type_beds = {
    "1 ROOM": 1, "2 ROOM": 1, "3 ROOM": 2,
    "4 ROOM": 3, "5 ROOM": 4, "EXECUTIVE": 4,
    "MULTI GENERATION": 5, "MULTI-GENERATION": 5,
}
df["beds"] = df["flat_type"].str.upper().map(flat_type_beds).fillna(3)

# Property age from remaining_lease ("61 years 04 months" → age 38)
def parse_age(s):
    try:
        years = int(str(s).split()[0])
        return 99 - years
    except Exception:
        return np.nan

df["property_age"] = df["remaining_lease"].apply(parse_age)

# Storey midpoint ("04 TO 06" → 5.0)
def parse_storey(s):
    try:
        lo, hi = str(s).split(" TO ")
        return (int(lo) + int(hi)) / 2
    except Exception:
        return 5.0

df["storey"] = df["storey_range"].apply(parse_storey)

# District
df["district"] = df["town"].str.upper().map(TOWN_TO_DISTRICT).fillna(15)

# Interaction features (same as valuation_model.py)
df["log_sqft"]      = np.log1p(df["sqft"])
df["beds_sqft"]     = df["beds"] * df["sqft"]
df["beds_sq"]       = df["beds"] ** 2
df["log_beds_sqft"] = np.log1p(df["beds_sqft"])
df["sqft_bin"]      = pd.qcut(df["sqft"], q=10, labels=False,
                               duplicates="drop").fillna(4).astype(float)
df["is_freehold"]   = 0
df["log_price"]     = np.log1p(df["resale_price"])

# Filter — time-align to listing snapshot period
df["year"] = pd.to_datetime(df["month"]).dt.year
n_before = len(df)
df = df[df["year"] >= YEAR_CUTOFF]
print(f"    Kept {len(df):,} rows from {YEAR_CUTOFF}–{CURRENT_YEAR} "
      f"(dropped {n_before - len(df):,} pre-{YEAR_CUTOFF} records)")

df = df[(df["resale_price"] >= 100_000) & (df["resale_price"] <= 2_000_000)]
df = df.dropna(subset=["resale_price", "sqft", "property_age", "district"])
print(f"    {len(df):,} rows after price filter + dropna")

# ── Train ─────────────────────────────────────────────────────────────────────
FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold",
    "property_age", "district",
]

from sklearn.model_selection import train_test_split, KFold, cross_val_score
import xgboost as xgb

df_m = df[FEATURES + ["log_price", "resale_price"]].dropna()
X, y_log, y_raw = df_m[FEATURES], df_m["log_price"], df_m["resale_price"]

X_train, X_test, y_log_tr, y_log_te, _, y_raw_te = train_test_split(
    X, y_log, y_raw, test_size=TEST_SIZE, random_state=RANDOM_STATE
)

print(f"\n[3] Training XGBoost on {len(X_train):,} rows ...")
model = xgb.XGBRegressor(
    n_estimators=500, learning_rate=0.05, max_depth=6,
    subsample=0.8, colsample_bytree=0.8,
    random_state=RANDOM_STATE, n_jobs=-1, verbosity=0,
)
model.fit(X_train, y_log_tr)

# 5-fold CV R² on training set (same as main pipeline)
print(f"    Running {CV_FOLDS}-fold CV ...")
kf = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
cv_scores = cross_val_score(
    xgb.XGBRegressor(
        n_estimators=500, learning_rate=0.05, max_depth=6,
        subsample=0.8, colsample_bytree=0.8,
        random_state=RANDOM_STATE, n_jobs=-1, verbosity=0,
    ),
    X_train, y_log_tr, cv=kf, scoring="r2"
)
cv_r2_mean = round(float(cv_scores.mean()), 4)
cv_r2_std  = round(float(cv_scores.std()),  4)
print(f"    CV R²: {cv_r2_mean:.4f} ± {cv_r2_std:.4f}")

# ── Evaluate ──────────────────────────────────────────────────────────────────
y_pred_log = model.predict(X_test)
y_pred_raw = np.expm1(y_pred_log)
test_mape  = round(mape(y_raw_te.values, y_pred_raw), 2)
test_r2    = round(float(1 - np.sum((y_log_te - y_pred_log)**2) /
                             np.sum((y_log_te - y_log_te.mean())**2)), 4)

# Reference numbers from main pipeline (listing-price model)
listing_mape   = 8.96
listing_r2     = 0.8877
listing_cv_r2  = 0.886
listing_n_test = 2198

delta_mape = test_mape - listing_mape

print(f"\n[4] Results — time-aligned comparison (≥{YEAR_CUTOFF}):")
print(f"")
print(f"    {'Metric':<25} {'Transacted':>12} {'Listing (main)':>15}")
print(f"    {'-'*54}")
print(f"    {'MAPE (%)':<25} {test_mape:>12.2f} {listing_mape:>15.2f}")
print(f"    {'Test R²':<25} {test_r2:>12.4f} {listing_r2:>15.4f}")
print(f"    {'CV R² (5-fold)':<25} {cv_r2_mean:>12.4f} {listing_cv_r2:>15.4f}")
print(f"    {'n_test':<25} {len(y_raw_te):>12,} {listing_n_test:>15,}")
print(f"")
print(f"    MAPE delta : {delta_mape:+.2f} pp")

if abs(delta_mape) < 3:
    verdict = "COMPARABLE — listing prices are a good proxy for transacted prices."
elif delta_mape < 0:
    verdict = "TRANSACTED BETTER — listing prices add systematic noise."
else:
    verdict = "LISTING BETTER — transacted data harder to fit with same features."
print(f"    Verdict    : {verdict}")

# ── Save ──────────────────────────────────────────────────────────────────────
result = {
    "year_cutoff": YEAR_CUTOFF,
    "transacted": {
        "mape": test_mape, "test_r2": test_r2,
        "cv_r2_mean": cv_r2_mean, "cv_r2_std": cv_r2_std,
        "n_train": len(X_train), "n_test": len(X_test),
    },
    "listing_main_pipeline": {
        "mape": listing_mape, "test_r2": listing_r2,
        "cv_r2_mean": listing_cv_r2, "n_test": listing_n_test,
    },
    "delta_mape_pp": round(delta_mape, 2),
    "verdict": verdict,
}
with open(OUT_JSON, "w") as f:
    json.dump(result, f, indent=2)
print(f"\n[5] Saved → {OUT_JSON}")
