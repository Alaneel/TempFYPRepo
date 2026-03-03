"""
Singapore Property Valuation Model Pipeline
============================================
FYP-level ML pipeline — trains SEPARATE models per (property_type × buy_rent).

Models:
  Condominium × sale,  Condominium × rent
  HDB         × sale,  HDB         × rent
  Landed      × sale,  Landed      × rent
  GCB         × sale,  GCB         × rent

Steps:
  0  Data extraction from PostgreSQL
  1  Data quality / EDA report
  2  Feature engineering (property_type dropped — each model is already pure)
  3  Per-segment train/test split
  4  Multi-model training  (Baseline → Ridge → RF → XGBoost → LightGBM)
  5  Evaluation  (MAE, RMSE, MAPE, R², 5-fold CV)
  6  SHAP interpretability  (global summary + local waterfall)
  7  Artifact export  (models, metrics.json, plots, HTML report)

Usage:
    python pipeline/valuation_model.py
    python pipeline/valuation_model.py --no-db   # use cached CSV
    python pipeline/valuation_model.py --quick   # skip LightGBM + local SHAP
"""

import argparse
import os
import json
import sys
import warnings
import time
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
import joblib

from sklearn.dummy import DummyRegressor
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR   = PROJECT_ROOT / "models" / "valuation"
CACHE_CSV    = OUTPUT_DIR / "listings_cache.csv"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "alanwang")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "real_estate_app")

# Canonical property type names (as stored in DB)
PROPERTY_TYPES = ["Condominium", "HDB", "Landed", "Good Class Bungalow"]
# Short dir-safe keys
TYPE_KEYS = {
    "Condominium":        "condo",
    "HDB":                "hdb",
    "Landed":             "landed",
    "Good Class Bungalow": "gcb",
}

SALE_PRICE_MAX  = 200_000_000
RENT_PRICE_MAX  = 150_000
SALE_PRICE_MIN  = 100_000
RENT_PRICE_MIN  = 100
PSF_MAX         = 20_000

CV_FOLDS     = 5
RANDOM_STATE = 42
TEST_SIZE    = 0.20
CURRENT_YEAR = 2026

# Minimum samples needed to train — skip segment if below this
MIN_SAMPLES = 100

# Features (NO property_type — each model is already pure)
NUMERIC_FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold",
    "property_age",   # CURRENT_YEAR - built_year (99.8% coverage)
    "district",       # Singapore district 1-28 (from reverse geocode, ~92% coverage)
]
CATEGORICAL_FEATURES = []   # no cat features after splitting by type
TARGET = "log_price"

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
_log_lines = []

def log(msg: str = ""):
    ts   = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _log_lines.append(line)

def section(title: str):
    bar = "=" * 60
    log(); log(bar); log(f"  {title}"); log(bar)


# ──────────────────────────────────────────────────────────────────────────────
# Step 0 — Data extraction
# ──────────────────────────────────────────────────────────────────────────────
def load_data(no_db: bool = False) -> pd.DataFrame:
    section("STEP 0 — Data Extraction")
    if no_db and CACHE_CSV.exists():
        log(f"Using cached CSV: {CACHE_CSV}")
        df = pd.read_csv(CACHE_CSV)
        log(f"Loaded {len(df):,} rows from cache.")
        return df
    try:
        from sqlalchemy import create_engine
        try:
            from dotenv import load_dotenv
            load_dotenv(PROJECT_ROOT / ".env")
        except ImportError:
            pass
        url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        log(f"Connecting to: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(url)
        query = """
            SELECT id, price, psf, beds, baths, sqft,
                   property_type, tenure, buy_rent, source,
                   built_year, district
            FROM listings
            WHERE price IS NOT NULL AND price > 0
        """
        df = pd.read_sql(query, engine)
        log(f"Loaded {len(df):,} rows from PostgreSQL.")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(CACHE_CSV, index=False)
        log(f"Cached to: {CACHE_CSV}")
        return df
    except Exception as e:
        log(f"DB connection failed: {e}")
        if CACHE_CSV.exists():
            log("Falling back to cached CSV.")
            return pd.read_csv(CACHE_CSV)
        raise RuntimeError("No data available.") from e


# ──────────────────────────────────────────────────────────────────────────────
# Step 1 — EDA
# ──────────────────────────────────────────────────────────────────────────────
def run_eda(df: pd.DataFrame, output_dir: Path) -> dict:
    section("STEP 1 — Data Quality & EDA")
    eda_dir = output_dir / "eda"
    eda_dir.mkdir(parents=True, exist_ok=True)
    stats = {}

    log("Missing value summary:")
    missing = pd.DataFrame({
        "column":      df.columns,
        "missing_n":   df.isnull().sum().values,
        "missing_pct": (df.isnull().sum() / len(df) * 100).round(1).values,
    })
    missing = missing[missing["missing_n"] > 0].sort_values("missing_pct", ascending=False)
    for _, row in missing.iterrows():
        log(f"  {row['column']:25s}  {row['missing_n']:6,}  ({row['missing_pct']:.1f}%)")
    stats["missing"] = missing.to_dict(orient="records")

    log("\nData split by property_type × buy_rent:")
    for pt in PROPERTY_TYPES:
        for br, label in [("property-for-sale", "sale"), ("property-for-rent", "rent")]:
            n = len(df[(df["property_type"] == pt) & (df["buy_rent"] == br)])
            log(f"  {pt:25s} {label:4s}  {n:5,}")
    stats["buy_rent_dist"] = df["buy_rent"].value_counts().to_dict()

    # ── EDA plot ──────────────────────────────────────────────────────────────
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("EDA — Price by Property Type", fontsize=15, fontweight="bold")

    palette = {"Condominium": "#3B82F6", "HDB": "#10B981", "Landed": "#F59E0B", "Good Class Bungalow": "#8B5CF6"}

    for ax, (br, label) in zip(axes[0], [("property-for-sale", "Sale"), ("property-for-rent", "Rent")]):
        sub = df[df["buy_rent"] == br].copy()
        price_cap = SALE_PRICE_MAX if "sale" in br else RENT_PRICE_MAX
        sub = sub[sub["price"].between(SALE_PRICE_MIN if "sale" in br else RENT_PRICE_MIN, price_cap)]
        for pt, color in palette.items():
            vals = np.log10(sub[sub["property_type"] == pt]["price"].dropna())
            if len(vals) > 5:
                ax.hist(vals, bins=40, alpha=0.55, color=color, label=pt, edgecolor="none")
        ax.set_xlabel("log₁₀(Price SGD)")
        ax.set_ylabel("Count")
        ax.set_title(f"Price Distribution — {label}")
        ax.legend(fontsize=7)

    for ax, (br, label) in zip(axes[1], [("property-for-sale", "Sale"), ("property-for-rent", "Rent")]):
        sub = df[(df["buy_rent"] == br) & df["sqft"].between(50, 30_000)]
        price_cap = SALE_PRICE_MAX if "sale" in br else RENT_PRICE_MAX
        sub = sub[sub["price"].between(SALE_PRICE_MIN if "sale" in br else RENT_PRICE_MIN, price_cap)]
        for pt, color in palette.items():
            s = sub[sub["property_type"] == pt].sample(min(500, len(sub[sub["property_type"] == pt])), random_state=42)
            if len(s) > 5:
                ax.scatter(np.log10(s["sqft"]), np.log10(s["price"]), alpha=0.25, s=4, color=color, label=pt)
        ax.set_xlabel("log₁₀(sqft)")
        ax.set_ylabel("log₁₀(price)")
        ax.set_title(f"sqft vs Price — {label}")
        ax.legend(fontsize=7)

    plt.tight_layout()
    eda_path = eda_dir / "eda_overview.png"
    fig.savefig(eda_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    log(f"\nEDA plot saved: {eda_path}")
    stats["eda_plot"] = str(eda_path)
    return stats


# ──────────────────────────────────────────────────────────────────────────────
# Step 2 — Feature engineering (per type, no property_type feature)
# ──────────────────────────────────────────────────────────────────────────────
def engineer_features(df: pd.DataFrame, property_type: str, mode: str) -> pd.DataFrame:
    price_min = SALE_PRICE_MIN if mode == "sale" else RENT_PRICE_MIN
    price_max = SALE_PRICE_MAX if mode == "sale" else RENT_PRICE_MAX
    br        = "property-for-sale" if mode == "sale" else "property-for-rent"

    df = df[
        (df["property_type"] == property_type) &
        (df["buy_rent"] == br) &
        (df["price"].between(price_min, price_max))
    ].copy()

    df["beds"] = pd.to_numeric(df["beds"], errors="coerce")
    if mode == "rent":
        df["beds"] = df["beds"].fillna(1).clip(0, 20)
    else:
        df["beds"] = df["beds"].fillna(df["beds"].median()).clip(0, 20)

    # Exclude beds=0 listings:
    #   Rent  → room-for-rent (avg $993-1,544/mo vs full-unit $3,500-7,500/mo)
    #   Sale  → bare land / commercial conversions without bedroom count
    # These are fundamentally different products and heavily bias the model.
    df = df[df["beds"] >= 1]

    df["sqft"] = pd.to_numeric(df["sqft"], errors="coerce")
    df = df[df["sqft"].between(50, 50_000)]

    df["is_freehold"] = df["tenure"].fillna("").str.lower().str.contains("freehold").astype(int)
    df["log_sqft"]    = np.log10(df["sqft"].clip(1))
    df["log_price"]   = np.log10(df["price"])

    df["beds_sqft"]      = df["beds"] * df["sqft"]
    df["beds_sq"]        = df["beds"] ** 2
    df["log_beds_sqft"]  = df["beds"] * df["log_sqft"]

    df["sqft_bin"] = pd.qcut(df["sqft"], q=5, labels=False, duplicates="drop")

    # property_age: extract numeric year from built_year string, e.g. "2014" → 12
    def _parse_year(val):
        if pd.isna(val):
            return np.nan
        digits = "".join(c for c in str(val) if c.isdigit())
        if len(digits) >= 4:
            yr = int(digits[:4])
            if 1960 <= yr <= CURRENT_YEAR + 5:
                return CURRENT_YEAR - yr
        return np.nan

    df["property_age"] = df["built_year"].apply(_parse_year)
    median_age = df["property_age"].median()
    df["property_age"] = df["property_age"].fillna(median_age if not np.isnan(median_age) else 10)

    # district: 1-28, fill missing with segment median (imputer also handles this,
    # but pre-fill here so the median is segment-specific, not global)
    df["district"] = pd.to_numeric(df["district"], errors="coerce")
    median_district = df["district"].median()
    df["district"] = df["district"].fillna(median_district if not np.isnan(median_district) else 15)

    return df


def build_preprocessor():
    num_pipe = Pipeline([
        ("impute", SimpleImputer(strategy="median")),
        ("scale",  StandardScaler()),
    ])
    return ColumnTransformer([("num", num_pipe, NUMERIC_FEATURES)], remainder="drop")


# ──────────────────────────────────────────────────────────────────────────────
# Metrics
# ──────────────────────────────────────────────────────────────────────────────
def mape(y_true, y_pred):
    y_t = 10 ** y_true; y_p = 10 ** y_pred
    return float(np.mean(np.abs((y_t - y_p) / y_t)) * 100)

def evaluate(name, model, X_test, y_test) -> dict:
    y_pred   = model.predict(X_test)
    mae_sgd  = mean_absolute_error(10 ** y_test, 10 ** y_pred)
    return {
        "model":    name,
        "MAE_log":  round(mean_absolute_error(y_test, y_pred), 4),
        "RMSE_log": round(np.sqrt(mean_squared_error(y_test, y_pred)), 4),
        "MAPE_pct": round(mape(y_test, y_pred), 2),
        "R2":       round(r2_score(y_test, y_pred), 4),
        "MAE_SGD":  int(mae_sgd),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Steps 4+5 — Train + Evaluate one segment
# ──────────────────────────────────────────────────────────────────────────────
def train_segment(
    property_type: str, mode: str, df_seg: pd.DataFrame,
    output_dir: Path, quick: bool = False
) -> "Optional[dict]":

    type_key  = TYPE_KEYS[property_type]
    seg_label = f"{type_key}_{mode}"
    seg_dir   = output_dir / seg_label
    seg_dir.mkdir(parents=True, exist_ok=True)

    if len(df_seg) < MIN_SAMPLES:
        log(f"  SKIP {seg_label} — only {len(df_seg)} samples (< {MIN_SAMPLES})")
        return None

    log(f"\n  [{seg_label}]  n={len(df_seg):,}")

    X = df_seg[NUMERIC_FEATURES]
    y = df_seg[TARGET]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    preprocessor = build_preprocessor()

    models = {
        "Baseline": Pipeline([("pre", preprocessor), ("m", DummyRegressor(strategy="median"))]),
        "Ridge":    Pipeline([("pre", preprocessor), ("m", Ridge(alpha=10.0))]),
        "RF":       Pipeline([("pre", preprocessor), ("m", RandomForestRegressor(
                                  n_estimators=200, max_depth=12, min_samples_leaf=5,
                                  n_jobs=-1, random_state=RANDOM_STATE))]),
    }
    try:
        from xgboost import XGBRegressor
        models["XGBoost"] = Pipeline([("pre", preprocessor), ("m", XGBRegressor(
            n_estimators=400, learning_rate=0.05, max_depth=5, subsample=0.8,
            colsample_bytree=0.8, reg_alpha=0.1, n_jobs=-1, random_state=RANDOM_STATE, verbosity=0))])
    except ImportError:
        pass

    if not quick:
        try:
            from lightgbm import LGBMRegressor
            models["LightGBM"] = Pipeline([("pre", preprocessor), ("m", LGBMRegressor(
                n_estimators=400, learning_rate=0.05, num_leaves=63, subsample=0.8,
                colsample_bytree=0.8, reg_alpha=0.1, n_jobs=-1, random_state=RANDOM_STATE, verbose=-1))])
        except ImportError:
            pass

    kf      = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
    results = []
    trained = {}

    for name, pipe in models.items():
        t0  = time.time()
        pipe.fit(X_train, y_train)
        row = evaluate(name, pipe, X_test, y_test)
        row["train_time_s"] = round(time.time() - t0, 1)
        cv  = cross_val_score(pipe, X_train, y_train, cv=kf, scoring="r2", n_jobs=-1)
        row["CV_R2_mean"] = round(cv.mean(), 4)
        row["CV_R2_std"]  = round(cv.std(), 4)
        results.append(row)
        trained[name] = pipe
        log(f"    {name:10s}  MAPE={row['MAPE_pct']:5.1f}%  R²={row['R2']:.4f}  "
            f"MAE=S${row['MAE_SGD']:,.0f}  CV={row['CV_R2_mean']:.3f}±{row['CV_R2_std']:.3f}")

    # Pick best (XGBoost > LightGBM > RF > Ridge)
    for pref in ["XGBoost", "LightGBM", "RF", "Ridge"]:
        if pref in trained:
            best_name = pref; break
    else:
        best_name = list(trained.keys())[-1]

    best = trained[best_name]
    best_metrics = next(r for r in results if r["model"] == best_name)

    # Save model
    model_path = seg_dir / "best_model.pkl"
    joblib.dump(best, model_path)

    # Save metrics.json so service can load MAPE per segment
    metrics = {
        "property_type": property_type,
        "mode":          mode,
        "best_model":    best_name,
        "n_train":       len(X_train),
        "n_test":        len(X_test),
        "MAPE_pct":      best_metrics["MAPE_pct"],
        "R2":            best_metrics["R2"],
        "MAE_SGD":       best_metrics["MAE_SGD"],
        "all_results":   results,
        "generated_at":  datetime.now().isoformat(),
    }
    (seg_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

    # Plots
    _plot_residuals(best, X_test, y_test, best_name, seg_label, seg_dir)
    _plot_pred_vs_actual(best, X_test, y_test, best_name, seg_label, seg_dir)

    return {
        "seg_label":       seg_label,
        "property_type":   property_type,
        "mode":            mode,
        "best_model_name": best_name,
        "best_model":      best,
        "X_test":          X_test,
        "y_test":          y_test,
        "X_train":         X_train,
        "y_train":         y_train,
        "results":         results,
        "seg_dir":         seg_dir,
        "metrics":         metrics,
    }


def _plot_residuals(model, X_test, y_test, name, seg_label, output_dir):
    y_pred = model.predict(X_test)
    residuals = y_test.values - y_pred
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    fig.suptitle(f"Residuals — {name} [{seg_label}]", fontweight="bold")
    axes[0].scatter(y_pred, residuals, alpha=0.25, s=6, color="#3B82F6")
    axes[0].axhline(0, color="red", linestyle="--")
    axes[0].set_xlabel("Predicted log₁₀(Price)"); axes[0].set_ylabel("Residual")
    axes[1].hist(residuals, bins=50, color="#8B5CF6", edgecolor="none")
    axes[1].axvline(0, color="red", linestyle="--")
    axes[1].set_xlabel("Residual"); axes[1].set_ylabel("Count")
    plt.tight_layout()
    fig.savefig(output_dir / f"residuals_{seg_label}.png", dpi=110)
    plt.close(fig)


def _plot_pred_vs_actual(model, X_test, y_test, name, seg_label, output_dir):
    y_pred = model.predict(X_test)
    lim = [min(y_test.min(), y_pred.min()) - 0.1, max(y_test.max(), y_pred.max()) + 0.1]
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.scatter(y_test, y_pred, alpha=0.25, s=6, color="#10B981")
    ax.plot(lim, lim, "r--", linewidth=1.5, label="Perfect")
    ax.set_xlim(lim); ax.set_ylim(lim)
    ax.set_xlabel("Actual log₁₀(Price)"); ax.set_ylabel("Predicted log₁₀(Price)")
    ax.set_title(f"Pred vs Actual — {name} [{seg_label}]")
    r2 = r2_score(y_test, y_pred)
    ax.text(0.05, 0.95, f"R² = {r2:.4f}", transform=ax.transAxes, va="top", fontsize=10,
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8))
    plt.tight_layout()
    fig.savefig(output_dir / f"pred_vs_actual_{seg_label}.png", dpi=110)
    plt.close(fig)


# ──────────────────────────────────────────────────────────────────────────────
# Step 6 — SHAP
# ──────────────────────────────────────────────────────────────────────────────
def run_shap(seg_result: dict, quick: bool = False):
    try:
        import shap
    except ImportError:
        return

    seg_label = seg_result["seg_label"]
    pipe      = seg_result["best_model"]
    X_test    = seg_result["X_test"]
    seg_dir   = seg_result["seg_dir"]
    model_name = seg_result["best_model_name"]

    pre = pipe.named_steps["pre"]
    X_t = pre.transform(X_test)
    X_df = pd.DataFrame(X_t, columns=NUMERIC_FEATURES)
    model_step = pipe.named_steps["m"]

    try:
        sample_size = min(800, len(X_df))
        X_sample = X_df.sample(sample_size, random_state=RANDOM_STATE)

        if any(k in model_name for k in ("XGBoost", "LightGBM", "RF", "Forest", "Gradient")):
            explainer = shap.TreeExplainer(model_step)
        else:
            explainer = shap.LinearExplainer(model_step, X_df)
        shap_vals = explainer.shap_values(X_sample)

        # Bar chart
        fig, _ = plt.subplots(figsize=(9, 6))
        shap.summary_plot(shap_vals, X_sample, feature_names=NUMERIC_FEATURES,
                          plot_type="bar", show=False, max_display=8)
        plt.title(f"SHAP Importance — {model_name} [{seg_label}]", fontsize=12, fontweight="bold")
        plt.tight_layout()
        bar_path = seg_dir / f"shap_bar_{seg_label}.png"
        fig.savefig(bar_path, dpi=110, bbox_inches="tight")
        plt.close("all")

        # Beeswarm
        fig, _ = plt.subplots(figsize=(9, 7))
        shap.summary_plot(shap_vals, X_sample, feature_names=NUMERIC_FEATURES,
                          show=False, max_display=8)
        plt.title(f"SHAP Beeswarm — {model_name} [{seg_label}]", fontsize=12, fontweight="bold")
        plt.tight_layout()
        bee_path = seg_dir / f"shap_beeswarm_{seg_label}.png"
        fig.savefig(bee_path, dpi=110, bbox_inches="tight")
        plt.close("all")

        log(f"    SHAP saved: {bar_path.name}")
        seg_result["shap_bar"]      = str(bar_path)
        seg_result["shap_beeswarm"] = str(bee_path)

    except Exception as e:
        log(f"    SHAP failed for {seg_label}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Step 7 — HTML Report
# ──────────────────────────────────────────────────────────────────────────────
def render_html_report(eda_stats: dict, seg_results: list, output_dir: Path, run_time: float):
    section("STEP 7 — HTML Report")

    def img_tag(path, caption, width="100%"):
        if not path or not Path(path).exists():
            return f"<p><em>{caption} — not generated</em></p>"
        return (f'<figure><img src="{path}" style="width:{width};border-radius:8px;'
                f'box-shadow:0 2px 8px rgba(0,0,0,.1)">'
                f'<figcaption>{caption}</figcaption></figure>')

    def metrics_rows(results):
        rows = []
        best = sorted(results, key=lambda x: x["MAPE_pct"])[0]["model"]
        for r in sorted(results, key=lambda x: x["MAPE_pct"]):
            badge = " 🏆" if r["model"] == best else ""
            rows.append(
                f"<tr><td><strong>{r['model']}{badge}</strong></td>"
                f"<td>{r['MAPE_pct']:.1f}%</td><td>{r['R2']:.4f}</td>"
                f"<td>S$ {r['MAE_SGD']:,}</td>"
                f"<td>{r.get('CV_R2_mean','?'):.4f}±{r.get('CV_R2_std','?'):.4f}</td></tr>"
            )
        return "\n".join(rows)

    seg_sections = ""
    for seg in seg_results:
        if seg is None:
            continue
        label = seg["seg_label"]
        seg_sections += f"""
        <section>
          <h2>{seg['property_type']} — {seg['mode'].title()}</h2>
          <p>Best model: <strong>{seg['best_model_name']}</strong>
             · MAPE <strong>{seg['metrics']['MAPE_pct']:.1f}%</strong>
             · R² <strong>{seg['metrics']['R2']:.4f}</strong>
             · MAE S${seg['metrics']['MAE_SGD']:,}
             · n_train={seg['metrics']['n_train']:,}</p>
          <table>
            <thead><tr><th>Model</th><th>MAPE</th><th>R²</th><th>MAE (SGD)</th><th>CV R²</th></tr></thead>
            <tbody>{metrics_rows(seg['results'])}</tbody>
          </table>
          <div class="img-grid">
            {img_tag(str(seg['seg_dir'] / f"residuals_{label}.png"), "Residuals")}
            {img_tag(str(seg['seg_dir'] / f"pred_vs_actual_{label}.png"), "Pred vs Actual")}
          </div>
          <div class="img-grid">
            {img_tag(seg.get('shap_bar',''), "SHAP Bar")}
            {img_tag(seg.get('shap_beeswarm',''), "SHAP Beeswarm")}
          </div>
        </section>"""

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Property Valuation Report — Per-Type Models</title>
<style>
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#F8FAFC;color:#1e293b;line-height:1.6;margin:0}}
  header{{background:linear-gradient(135deg,#3B82F6,#8B5CF6);color:#fff;padding:36px 60px}}
  header h1{{font-size:1.9rem;font-weight:800}}
  main{{max-width:1100px;margin:32px auto;padding:0 24px}}
  section{{background:#fff;border-radius:14px;padding:28px;margin-bottom:28px;box-shadow:0 2px 10px rgba(0,0,0,.07)}}
  h2{{font-size:1.25rem;font-weight:700;color:#3B82F6;border-bottom:2px solid #e2e8f0;padding-bottom:6px;margin-bottom:16px}}
  table{{width:100%;border-collapse:collapse;font-size:.88rem;margin:12px 0}}
  th{{background:#f1f5f9;padding:8px 12px;text-align:left;font-weight:600}}
  td{{padding:8px 12px;border-bottom:1px solid #e2e8f0}}
  .img-grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}}
  figure{{margin:0}} figcaption{{font-size:.8rem;color:#64748b;text-align:center;margin-top:4px}}
  footer{{text-align:center;padding:28px;color:#94a3b8;font-size:.85rem}}
</style></head><body>
<header>
  <h1>🏠 Property Valuation — Per Property-Type Models</h1>
  <div style="opacity:.8;font-size:.9rem;margin-top:6px">
    Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} · Runtime: {run_time:.0f}s · FYP 2026
  </div>
</header>
<main>{seg_sections}</main>
<footer>Singapore Property Valuation Model Pipeline · Separate models per property type</footer>
</body></html>"""

    report_path = output_dir / "evaluation_report.html"
    report_path.write_text(html, encoding="utf-8")
    log(f"HTML report saved: {report_path}")
    return report_path


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Singapore Property Valuation Pipeline")
    parser.add_argument("--no-db",  action="store_true")
    parser.add_argument("--quick",  action="store_true")
    args = parser.parse_args()

    t_start = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 0
    df_raw = load_data(no_db=args.no_db)

    # Step 1
    eda_stats = run_eda(df_raw, OUTPUT_DIR)

    # Steps 2–6: iterate over all (property_type × mode) segments
    section("STEPS 2-6 — Training All Segments")
    all_results = []

    for property_type in PROPERTY_TYPES:
        for mode in ["sale", "rent"]:
            df_seg = engineer_features(df_raw, property_type, mode)
            result = train_segment(property_type, mode, df_seg, OUTPUT_DIR, quick=args.quick)
            if result:
                run_shap(result, quick=args.quick)
            all_results.append(result)

    # Step 7
    run_time = time.time() - t_start
    render_html_report(eda_stats, all_results, OUTPUT_DIR, run_time)

    log_path = OUTPUT_DIR / "pipeline_log.txt"
    log_path.write_text("\n".join(_log_lines), encoding="utf-8")

    section("PIPELINE COMPLETE")
    log(f"Total runtime: {run_time:.0f}s")
    log(f"Model dirs:")
    for pt in PROPERTY_TYPES:
        for mode in ["sale", "rent"]:
            key = TYPE_KEYS[pt]
            p   = OUTPUT_DIR / f"{key}_{mode}" / "best_model.pkl"
            status = "✓" if p.exists() else "✗ SKIPPED"
            log(f"  {key}_{mode}: {status}")
    log(f"\nOpen report: {OUTPUT_DIR / 'evaluation_report.html'}")


if __name__ == "__main__":
    main()
