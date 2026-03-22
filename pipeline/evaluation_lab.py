"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                FYP CHAPTER 5 — EVALUATION LAB                              ║
║                                                                            ║
║  Complete reproducible evaluation for:                                     ║
║    Part 1: Dataset & Environment Summary         → Tables 5.1, 5.2, 5.3   ║
║    Part 2: Valuation Model Evaluation            → Tables 5.4, 5.4b, 5.5  ║
║    Part 3: Recommendation Engine Evaluation      → Tables 5.6, 5.8, 5.9   ║
║                                                                            ║
║  Usage:                                                                    ║
║    python pipeline/evaluation_lab.py              # run everything         ║
║    python pipeline/evaluation_lab.py --part 1     # dataset summary only   ║
║    python pipeline/evaluation_lab.py --part 2     # valuation only         ║
║    python pipeline/evaluation_lab.py --part 3     # recommendation only    ║
║    python pipeline/evaluation_lab.py --no-db      # use cached CSV         ║
║                                                                            ║
║  Output:                                                                   ║
║    models/evaluation_lab_results.md               # all tables in markdown ║
║    Terminal output (screenshot this for your report appendix)              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import json
import platform
import warnings
import argparse
import asyncio
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# Project paths
# ──────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR   = PROJECT_ROOT / "models"
CACHE_CSV    = OUTPUT_DIR / "valuation" / "listings_cache.csv"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "real_estate_fyp")

VALID_TYPES  = {"Condominium", "HDB", "Landed", "Good Class Bungalow"}
TYPE_KEYS    = {"Condominium": "condo", "HDB": "hdb", "Landed": "landed", "Good Class Bungalow": "gcb"}
RANDOM_STATE = 42
TEST_SIZE    = 0.20
CV_FOLDS     = 5
MIN_SAMPLES  = 100
CURRENT_YEAR = 2025

NUMERIC_FEATURES = [
    "beds", "sqft", "log_sqft", "beds_sqft", "beds_sq",
    "log_beds_sqft", "sqft_bin", "is_freehold", "property_age", "district",
]
TARGET = "log_price"

# Add backend to path for recommendation eval
sys.path.insert(0, str(PROJECT_ROOT / "backend"))

# ──────────────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────────────
_md = []   # markdown accumulator
_start = time.time()

def _ts():
    return f"[{datetime.now().strftime('%H:%M:%S')}]"

def emit(msg=""):
    print(msg)
    _md.append(msg)

def banner(title):
    w = 70
    emit("")
    emit("=" * w)
    emit(f"  {title}")
    emit("=" * w)
    _md.append(f"\n## {title}\n")

def md_table(headers, rows):
    """Print + record a markdown table."""
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(str(c)))
    def fmt(cells):
        return "| " + " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cells)) + " |"
    sep = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
    hdr = fmt(headers)
    emit(hdr); emit(sep)
    for r in rows:
        emit(fmt(r))
    emit("")

# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────
def load_data(use_db=True):
    """Load listings from PostgreSQL or cached CSV."""
    if use_db:
        try:
            from sqlalchemy import create_engine
            url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            emit(f"{_ts()} Connecting to PostgreSQL: {url.replace(DB_PASS, '***') if DB_PASS else url}")
            engine = create_engine(url)
            query = """
                SELECT id, price, psf, beds, baths, sqft,
                       property_type, tenure, buy_rent, source,
                       built_year, district
                FROM listings
                WHERE price IS NOT NULL AND price > 0
            """
            df = pd.read_sql(query, engine)
            emit(f"{_ts()} Loaded {len(df):,} listings from PostgreSQL.")
            return df, engine
        except Exception as e:
            emit(f"{_ts()} DB connection failed: {e}")
            emit(f"{_ts()} Falling back to cached CSV...")

    if CACHE_CSV.exists():
        df = pd.read_csv(CACHE_CSV)
        emit(f"{_ts()} Loaded {len(df):,} listings from cached CSV: {CACHE_CSV}")
        return df, None
    raise RuntimeError("No data available — run valuation_model.py first or check DB.")


# ══════════════════════════════════════════════════════════════════════════════
# PART 1: Dataset & Environment Summary (Tables 5.1, 5.2, 5.3)
# ══════════════════════════════════════════════════════════════════════════════
def run_part1(df, engine):
    banner("PART 1: DATASET & ENVIRONMENT SUMMARY")

    # ── Table 5.1 ─────────────────────────────────────────────────────────
    emit(f"\n{_ts()} Generating Table 5.1 — Dataset Summary\n")
    total = len(df)
    rows_51 = [("Listings (total)", "PropertyGuru, 99.co, EdgeProp, SRX", f"{total:,}", "Combined marketplace data")]

    for pt in ["Condominium", "HDB", "Landed", "Good Class Bungalow"]:
        for mode_label, br_val in [("Sale", "property-for-sale"), ("Rent", "property-for-rent")]:
            n = len(df[(df["property_type"] == pt) & (df["buy_rent"] == br_val)])
            if n > 0:
                rows_51.append((f"{pt} – {mode_label}", "Scraped listings", f"{n:,}", "Valuation / Recommendation"))

    # Directory counts
    if engine is not None:
        from sqlalchemy import text
        with engine.connect() as conn:
            try:
                condo_n = conn.execute(text("SELECT COUNT(*) FROM condo_basic")).scalar()
            except: condo_n = "N/A"
            try:
                hdb_n = conn.execute(text("SELECT COUNT(*) FROM hdb_basic")).scalar()
            except: hdb_n = "N/A"
        rows_51.append(("Condo Directory (condo_basic)", "Scraped from 99.co", f"{condo_n:,}" if isinstance(condo_n, int) else condo_n, "Master reference table"))
        rows_51.append(("HDB Directory (hdb_basic)", "data.gov.sg", f"{hdb_n:,}" if isinstance(hdb_n, int) else hdb_n, "Master reference table"))
    else:
        rows_51.append(("Condo Directory (condo_basic)", "Scraped from 99.co", "~3,500+", "Master reference table"))
        rows_51.append(("HDB Directory (hdb_basic)", "data.gov.sg", "~10,000+", "Master reference table"))

    rows_51.append(("Synthetic User Profiles", "Hand-crafted", "3", "Recommendation evaluation"))

    emit("**Table 5.1: Summary of Datasets Used for Evaluation**\n")
    md_table(["Dataset", "Source", "Records", "Purpose"], rows_51)

    # ── Table 5.2 ─────────────────────────────────────────────────────────
    emit(f"{_ts()} Generating Table 5.2 — Baselines\n")
    emit("**Table 5.2: Baselines and Comparison Methods**\n")
    md_table(["Component", "Method", "Description"], [
        ("Valuation", "Baseline (DummyRegressor)", "Predicts median listing price; naive lower-bound"),
        ("Valuation", "Ridge Regression", "Linear model with L2 regularisation"),
        ("Valuation", "Random Forest", "Ensemble of 200 decision trees, max_depth=12"),
        ("Valuation", "XGBoost", "Gradient-boosted trees (400 rounds, lr=0.05, depth=5)"),
        ("Valuation", "LightGBM", "Histogram-based gradient boosting (400 rounds, 63 leaves)"),
        ("Recommendation", "Hybrid 6-dimension scoring", "Type(20%) + District(15%) + Price(20%) + Beds(15%) + Facilities(15%) + Bargain(15%)"),
    ])

    # ── Table 5.3 ─────────────────────────────────────────────────────────
    emit(f"{_ts()} Generating Table 5.3 — Environment\n")
    def _ver(mod):
        try:
            m = __import__(mod); return m.__version__
        except: return "N/A"

    emit("**Table 5.3: Implementation and Experimental Environment**\n")
    md_table(["Component", "Technology", "Version / Detail"], [
        ("Language (Backend)", "Python", platform.python_version()),
        ("Language (Frontend)", "TypeScript / Next.js", "React 18+"),
        ("Database", "PostgreSQL", f"{DB_NAME} @ {DB_HOST}:{DB_PORT}"),
        ("ML: XGBoost", "xgboost", _ver("xgboost")),
        ("ML: LightGBM", "lightgbm", _ver("lightgbm")),
        ("ML: scikit-learn", "sklearn", _ver("sklearn")),
        ("Explainability", "shap (TreeExplainer)", _ver("shap")),
        ("Data Processing", "pandas", _ver("pandas")),
        ("Operating System", platform.system(), platform.release()),
        ("Dataset Size", f"{len(df):,} listings", "After ETL cleaning & deduplication"),
        ("Evaluation Setting", "Offline", f"Seed={RANDOM_STATE}, 80/20 split, {CV_FOLDS}-fold CV"),
    ])


# ══════════════════════════════════════════════════════════════════════════════
# PART 2: Valuation Model Evaluation (Tables 5.4, 5.4b, 5.5)
# ══════════════════════════════════════════════════════════════════════════════

# Feature engineering (identical to valuation_model.py)
def _engineer(df, property_type, mode):
    price_min = 100_000 if mode == "sale" else 100
    price_max = 200_000_000 if mode == "sale" else 150_000
    br = "property-for-sale" if mode == "sale" else "property-for-rent"

    df = df[(df["property_type"] == property_type) & (df["buy_rent"] == br) &
            (df["price"].between(price_min, price_max))].copy()

    df["beds"] = pd.to_numeric(df["beds"], errors="coerce")
    df["beds"] = df["beds"].fillna(1 if mode == "rent" else df["beds"].median()).clip(0, 20)
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

    def _parse_year(val):
        if pd.isna(val): return np.nan
        digits = "".join(c for c in str(val) if c.isdigit())
        if len(digits) >= 4:
            yr = int(digits[:4])
            if 1960 <= yr <= CURRENT_YEAR + 5:
                return CURRENT_YEAR - yr
        return np.nan

    df["property_age"] = df["built_year"].apply(_parse_year)
    df["property_age"] = pd.to_numeric(df["property_age"], errors="coerce")
    med_age = df["property_age"].median()
    if pd.isna(med_age):
        med_age = 10
    df["property_age"] = df["property_age"].fillna(med_age)

    # district: 1-28, fill missing with segment median
    df["district"] = pd.to_numeric(df["district"], errors="coerce")
    med_dist = df["district"].median()
    if pd.isna(med_dist):
        med_dist = 15
    df["district"] = df["district"].fillna(med_dist)

    return df


def _mape(y_true, y_pred):
    yt = 10 ** y_true; yp = 10 ** y_pred
    return float(np.mean(np.abs((yt - yp) / yt)) * 100)


def run_part2(df):
    banner("PART 2: VALUATION MODEL EVALUATION")

    from sklearn.dummy import DummyRegressor
    from sklearn.linear_model import Ridge
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split, cross_val_score, KFold
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.metrics import mean_absolute_error, r2_score

    def _build_preprocessor():
        num_pipe = Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())])
        return ColumnTransformer([("num", num_pipe, NUMERIC_FEATURES)], remainder="drop")

    emit(f"\n{_ts()} Configuration:")
    emit(f"   Train/Test split: {int((1-TEST_SIZE)*100)}/{int(TEST_SIZE*100)}")
    emit(f"   Random seed: {RANDOM_STATE}")
    emit(f"   Cross-validation: {CV_FOLDS}-fold")
    emit(f"   Min samples per segment: {MIN_SAMPLES}")
    emit(f"   Models: Baseline, Ridge, Random Forest, XGBoost, LightGBM")
    emit(f"   Target: log10(price)")
    emit(f"   Features: {', '.join(NUMERIC_FEATURES)}")
    emit("")

    all_segment_results = {}  # seg_label → { n, models: [{ name, mape, r2, mae, cv_mean, cv_std }] }

    segments = [
        ("Condominium", "sale"), ("Condominium", "rent"),
        ("HDB", "sale"), ("HDB", "rent"),
        ("Landed", "sale"), ("Landed", "rent"),
        ("Good Class Bungalow", "sale"), ("Good Class Bungalow", "rent"),
    ]

    for pt, mode in segments:
        seg_label = f"{TYPE_KEYS[pt]}_{mode}"
        df_seg = _engineer(df.copy(), pt, mode)

        if len(df_seg) < MIN_SAMPLES:
            emit(f"{_ts()} SKIP {seg_label} — only {len(df_seg)} usable samples (< {MIN_SAMPLES})")
            all_segment_results[seg_label] = {"n": len(df_seg), "models": [], "skipped": True}
            continue

        emit(f"\n{_ts()} Training segment: [{seg_label}]  n = {len(df_seg):,}")
        emit(f"{'':>4}{'Model':<12} {'MAPE':>6}  {'R²':>7}  {'MAE (S$)':>12}  {'CV R² (5-fold)':>16}")
        emit(f"{'':>4}{'-'*12} {'-'*6}  {'-'*7}  {'-'*12}  {'-'*16}")

        X = df_seg[NUMERIC_FEATURES]
        y = df_seg[TARGET]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
        )

        pre = _build_preprocessor()
        models_dict = {
            "Baseline": Pipeline([("pre", pre), ("m", DummyRegressor(strategy="median"))]),
            "Ridge":    Pipeline([("pre", _build_preprocessor()), ("m", Ridge(alpha=10.0))]),
            "RF":       Pipeline([("pre", _build_preprocessor()), ("m", RandomForestRegressor(
                            n_estimators=200, max_depth=12, min_samples_leaf=5,
                            n_jobs=-1, random_state=RANDOM_STATE))]),
        }
        try:
            from xgboost import XGBRegressor
            models_dict["XGBoost"] = Pipeline([("pre", _build_preprocessor()), ("m", XGBRegressor(
                n_estimators=400, learning_rate=0.05, max_depth=5, subsample=0.8,
                colsample_bytree=0.8, reg_alpha=0.1, n_jobs=-1, random_state=RANDOM_STATE, verbosity=0))])
        except ImportError:
            emit(f"{'':>4}[WARN] XGBoost not installed, skipping.")
        try:
            from lightgbm import LGBMRegressor
            models_dict["LightGBM"] = Pipeline([("pre", _build_preprocessor()), ("m", LGBMRegressor(
                n_estimators=400, learning_rate=0.05, num_leaves=63, subsample=0.8,
                colsample_bytree=0.8, reg_alpha=0.1, n_jobs=-1, random_state=RANDOM_STATE, verbose=-1))])
        except ImportError:
            emit(f"{'':>4}[WARN] LightGBM not installed, skipping.")

        kf = KFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_STATE)
        seg_models = []

        for name, pipe in models_dict.items():
            t0 = time.time()
            pipe.fit(X_train, y_train)
            y_pred = pipe.predict(X_test)

            mape_val = round(_mape(y_test, y_pred), 1)
            r2_val   = round(r2_score(y_test, y_pred), 4)
            mae_val  = int(mean_absolute_error(10 ** y_test, 10 ** y_pred))
            cv       = cross_val_score(pipe, X_train, y_train, cv=kf, scoring="r2", n_jobs=-1)
            cv_mean  = round(cv.mean(), 3)
            cv_std   = round(cv.std(), 3)
            elapsed  = round(time.time() - t0, 1)

            seg_models.append({
                "name": name, "mape": mape_val, "r2": r2_val,
                "mae": mae_val, "cv_mean": cv_mean, "cv_std": cv_std
            })

            emit(f"{'':>4}{name:<12} {mape_val:>5.1f}%  {r2_val:>7.4f}  S${mae_val:>10,}  {cv_mean:>6.3f} ± {cv_std:.3f}  ({elapsed}s)")

        all_segment_results[seg_label] = {"n": len(df_seg), "models": seg_models}

    # ── Print Table 5.4 ───────────────────────────────────────────────────
    emit("")
    banner("TABLE 5.4: Valuation Model Comparison (Condominium Sale)")

    if "condo_sale" in all_segment_results and all_segment_results["condo_sale"]["models"]:
        seg = all_segment_results["condo_sale"]
        emit(f"\n**Segment: Condominium Sale  |  n = {seg['n']:,}  |  Split: 80/20  |  Seed: {RANDOM_STATE}**\n")
        best_mape = min(m["mape"] for m in seg["models"])
        rows = []
        for m in seg["models"]:
            star = " ★" if m["mape"] == best_mape else ""
            rows.append((
                f"{m['name']}{star}", f"{m['mape']}%", f"{m['r2']:.4f}",
                f"S${m['mae']:,}", f"{m['cv_mean']:.3f} ± {m['cv_std']:.3f}"
            ))
        md_table(["Model", "MAPE", "R²", "MAE (S$)", "5-Fold CV R²"], rows)

    # Also print condo_rent if available
    if "condo_rent" in all_segment_results and all_segment_results["condo_rent"]["models"]:
        seg = all_segment_results["condo_rent"]
        emit(f"\n**Segment: Condominium Rent  |  n = {seg['n']:,}**\n")
        best_mape = min(m["mape"] for m in seg["models"])
        rows = []
        for m in seg["models"]:
            star = " ★" if m["mape"] == best_mape else ""
            rows.append((
                f"{m['name']}{star}", f"{m['mape']}%", f"{m['r2']:.4f}",
                f"S${m['mae']:,}", f"{m['cv_mean']:.3f} ± {m['cv_std']:.3f}"
            ))
        md_table(["Model", "MAPE", "R²", "MAE (S$)", "5-Fold CV R²"], rows)

    # Also print landed_rent if available
    if "landed_rent" in all_segment_results and all_segment_results["landed_rent"]["models"]:
        seg = all_segment_results["landed_rent"]
        emit(f"\n**Segment: Landed Rent  |  n = {seg['n']:,}**\n")
        best_mape = min(m["mape"] for m in seg["models"])
        rows = []
        for m in seg["models"]:
            star = " ★" if m["mape"] == best_mape else ""
            rows.append((
                f"{m['name']}{star}", f"{m['mape']}%", f"{m['r2']:.4f}",
                f"S${m['mae']:,}", f"{m['cv_mean']:.3f} ± {m['cv_std']:.3f}"
            ))
        md_table(["Model", "MAPE", "R²", "MAE (S$)", "5-Fold CV R²"], rows)

    # ── Print Table 5.4b ──────────────────────────────────────────────────
    banner("TABLE 5.4b: Valuation Results Across All Segments")
    summary_rows = []
    for seg_label in ["condo_sale", "condo_rent", "landed_rent",
                      "hdb_sale", "hdb_rent", "landed_sale", "gcb_sale", "gcb_rent"]:
        if seg_label not in all_segment_results:
            continue
        seg = all_segment_results[seg_label]
        pretty = seg_label.replace("_", " ").title()
        if seg.get("skipped") or not seg["models"]:
            summary_rows.append((pretty, str(seg["n"]), "—", "Skipped", "—", f"(n < {MIN_SAMPLES})"))
        else:
            best = min(seg["models"], key=lambda m: m["mape"])
            summary_rows.append((
                pretty, f"{seg['n']:,}", best["name"],
                f"{best['mape']}%", f"{best['r2']:.3f}",
                f"{best['cv_mean']:.3f} ± {best['cv_std']:.3f}"
            ))
    md_table(["Segment", "n", "Best Model", "MAPE", "R²", "CV R²"], summary_rows)

    # ── Print Table 5.5 ──────────────────────────────────────────────────
    banner("TABLE 5.5: Sources of Valuation Error")

    # Compute actual missing-value percentages from the dataset
    total_n = len(df)
    tenure_miss = round(df["tenure"].isna().sum() / total_n * 100, 1)
    built_miss  = round(df["built_year"].isna().sum() / total_n * 100, 1)

    emit(f"\n{_ts()} Missing value analysis from loaded dataset (n={total_n:,}):")
    emit(f"   tenure:     {df['tenure'].isna().sum():,} missing ({tenure_miss}%)")
    emit(f"   built_year: {df['built_year'].isna().sum():,} missing ({built_miss}%)")
    emit(f"   beds:       {df['beds'].isna().sum():,} missing ({round(df['beds'].isna().sum()/total_n*100,1)}%)")
    emit(f"   sqft:       {df['sqft'].isna().sum():,} missing ({round(df['sqft'].isna().sum()/total_n*100,1)}%)")
    emit("")

    md_table(["Error Source", "Description", "Impact"], [
        ("Missing interior data", "Renovation, furnishing, floor level not in scraped data", "High"),
        ("Listing vs. transaction", "Model predicts asking price (includes negotiation buffer)", "Medium"),
        ("Data incompleteness", f"{tenure_miss}% missing tenure; {built_miss}% missing built_year", "Medium"),
        ("Cross-portal noise", "Residual naming/area inconsistencies across portals", "Low"),
        ("Temporal snapshot", "Point-in-time scrape; no time-series trends captured", "Low"),
    ])

    return all_segment_results


# ══════════════════════════════════════════════════════════════════════════════
# PART 3: Recommendation Engine Evaluation (Tables 5.6, 5.8, 5.9)
# ══════════════════════════════════════════════════════════════════════════════

SYNTHETIC_USERS = {
    "User A: East Coast Family Upgrader": {
        "goal": "3BR Sale in D15, ~$2,000,000",
        "profile": {
            "property_types": {"Condominium": 5},
            "districts": {15: 5},
            "avg_price": 2000000,
            "avg_beds": 3,
            "buy_rent": "property-for-sale",
            "total_favs": 5,
            "facilities": {"pool": 5, "gym": 5, "tennis": 0, "security": 5, "parking": 5}
        }
    },
    "User B: Luxury Landed Buyer": {
        "goal": "5BR Sale in D10, ~$8,000,000",
        "profile": {
            "property_types": {"Landed": 5, "Good Class Bungalow": 2},
            "districts": {10: 4, 11: 3},
            "avg_price": 8000000,
            "avg_beds": 5,
            "buy_rent": "property-for-sale",
            "total_favs": 7,
            "facilities": {"pool": 2, "gym": 0, "tennis": 0, "security": 0, "parking": 7}
        }
    },
    "User C: Budget Expat Rental": {
        "goal": "2BR Rent in D2, ~$4,500/mo",
        "profile": {
            "property_types": {"Condominium": 5, "HDB": 2},
            "districts": {2: 3, 3: 2},
            "avg_price": 4500,
            "avg_beds": 2,
            "buy_rent": "property-for-rent",
            "total_favs": 5,
            "facilities": {"pool": 5, "gym": 5, "tennis": 0, "security": 0, "parking": 0}
        }
    }
}


def run_part3():
    banner("PART 3: RECOMMENDATION ENGINE EVALUATION")

    # ── Table 5.8: Synthetic User Profiles ────────────────────────────────
    emit(f"\n{_ts()} Generating Table 5.8 — Synthetic User Profiles\n")
    emit("**Table 5.8: Synthetic User Profiles**\n")
    prof_rows = []
    for name, data in SYNTHETIC_USERS.items():
        p = data["profile"]
        short = name.split(":")[0].strip()
        persona = name.split(":")[1].strip() if ":" in name else name
        types = ", ".join(p["property_types"].keys())
        mode = "Sale" if "sale" in p["buy_rent"] else "Rent"
        budget = f"${p['avg_price']:,}" if p["avg_price"] >= 10000 else f"${p['avg_price']:,}/mo"
        districts = ", ".join(f"D{d}" for d in p["districts"].keys())
        facs = ", ".join(k.title() for k, v in p["facilities"].items() if v > 0)
        prof_rows.append((short, persona, types, mode, budget, str(int(p["avg_beds"])), districts, facs or "None"))
    md_table(["Profile", "Persona", "Type", "Mode", "Budget", "Beds", "Districts", "Facilities"], prof_rows)

    # ── Run recommendation scoring against live DB ────────────────────────
    emit(f"\n{_ts()} Running recommendation evaluation against live database...\n")

    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_ROOT / "backend" / ".env")
    except ImportError:
        pass

    from app.database import AsyncSessionLocal
    from sqlalchemy.future import select
    from sqlalchemy.orm import selectinload
    from app.models.listing import Listing
    from app.models.hdb import HdbBasic
    from app.models.condo import CondoBasic
    from app.models.agent import Agent
    from app.routers.recommendations import _score_candidate

    all_eval = {}

    async def _run():
        async with AsyncSessionLocal() as session:
            query = select(Listing).options(
                selectinload(Listing.agent),
                selectinload(Listing.condo)
            ).where(Listing.is_active == True).limit(5000)

            result = await session.execute(query)
            candidates = result.scalars().all()
            emit(f"{_ts()} Loaded {len(candidates):,} active listings from database.")
            emit(f"{_ts()} Scoring each listing across 6 dimensions...\n")

            for user_name, data in SYNTHETIC_USERS.items():
                profile = data["profile"]
                short = user_name.split(":")[0].strip()
                goal  = data["goal"]

                emit(f"   Testing: {user_name}")
                emit(f"   Goal: {goal}\n")

                scored = []
                for c in candidates:
                    if c.buy_rent != profile["buy_rent"]:
                        continue
                    score, reasons, val = _score_candidate(c, profile)
                    if score > 0:
                        scored.append((score, reasons, c))

                scored.sort(key=lambda x: x[0], reverse=True)
                top5 = scored[:5]

                # --- NDCG@5 Calculation ---
                def _get_relevance(cand, prof):
                    rel = 0
                    # Type Match (3 pts)
                    if str(cand.property_type) in prof["property_types"]: rel += 3
                    # District Match (3 pts)
                    if cand.district in prof["districts"]: rel += 3
                    # Beds Match (2 pts)
                    if cand.beds == prof["avg_beds"]: rel += 2
                    # Price Match (2 pts if within 15%)
                    if cand.price and prof["avg_price"]:
                        diff = abs(cand.price - prof["avg_price"]) / prof["avg_price"]
                        if diff <= 0.15: rel += 2
                        elif diff <= 0.30: rel += 1
                    return rel

                def _calculate_ndcg(top_list, prof, all_candidates):
                    # DCG@5
                    dcg = 0
                    for i, (score, reasons, cand) in enumerate(top_list, 1):
                        rel = _get_relevance(cand, prof)
                        dcg += (2**rel - 1) / np.log2(i + 1)
                    
                    # IDCG@5 (Ideal DCG)
                    # We need the top 5 possible relevance scores from ALL candidates
                    all_rels = sorted([_get_relevance(c, prof) for c in all_candidates], reverse=True)
                    idcg = 0
                    for i, rel in enumerate(all_rels[:5], 1):
                        idcg += (2**rel - 1) / np.log2(i + 1)
                    
                    return dcg / idcg if idcg > 0 else 0

                # Filter all_candidates for current mode for IDCG
                mode_candidates = [c for c in candidates if c.buy_rent == profile["buy_rent"]]
                ndcg_5 = _calculate_ndcg(top5, profile, mode_candidates)

                emit(f"   {'Rank':<5} {'Score':<7} {'Title':<28} {'Type':<14} {'Dist':<5} {'Price':<13} Beds")
                emit(f"   {'-'*5} {'-'*7} {'-'*28} {'-'*14} {'-'*5} {'-'*13} {'-'*4}")

                top5_data = []
                for idx, (score, reasons, c) in enumerate(top5, 1):
                    price_str = f"${c.price:,.0f}" if c.price else "N/A"
                    dist_str  = f"D{c.district}" if c.district else "D-"
                    beds_str  = str(c.beds) if c.beds is not None else "-"
                    title_str = (c.title or "—")[:28]
                    ptype_str = (str(c.property_type) or "-")[:14]

                    emit(f"   {idx:<5} {score:<7.3f} {title_str:<28} {ptype_str:<14} {dist_str:<5} {price_str:<13} {beds_str} BR")
                    top5_data.append({
                        "rank": idx, "score": score, "title": c.title,
                        "type": str(c.property_type), "price": c.price,
                        "beds": c.beds, "district": c.district
                    })

                emit(f"\n   NDCG@5: {ndcg_5:.3f}")
                emit(f"   Total candidates scored: {len(scored):,}")
                emit(f"   Score range: {scored[-1][0]:.3f} – {scored[0][0]:.3f}" if scored else "")
                emit("   " + "=" * 70 + "\n")

                all_eval[short] = {"goal": goal, "top5": top5_data, "total": len(scored), "ndcg_5": ndcg_5}

    asyncio.run(_run())

    # ── Table 5.6: Formatted results ─────────────────────────────────────
    banner("TABLE 5.6: Top-5 Recommendations per Synthetic User")

    table_rows = []
    for user_name, data in SYNTHETIC_USERS.items():
        short = user_name.split(":")[0].strip()
        if short not in all_eval:
            continue
        ev = all_eval[short]
        table_rows.append((f"**{user_name} ({ev['goal']})**", "", f"**NDCG@5: {ev['ndcg_5']:.3f}**", "", "", ""))
        for r in ev["top5"]:
            price_str = f"${r['price']:,.0f}" if r["price"] else "N/A"
            table_rows.append((
                str(r["rank"]), f"{r['score']:.3f}",
                (r["title"] or "—")[:30], r["type"][:14],
                price_str, f"{r['beds']} BR" if r["beds"] else "-"
            ))
    md_table(["Rank", "Score", "Property", "Type", "Price", "Beds"], table_rows)

    # ── Table 5.9: Summary Findings ──────────────────────────────────────
    banner("TABLE 5.9: Summary Findings")

    findings_rows = []
    for user_name, data in SYNTHETIC_USERS.items():
        short = user_name.split(":")[0].strip()
        if short not in all_eval or not all_eval[short]["top5"]:
            continue
        ev = all_eval[short]
        top1 = ev["top5"][0]
        price_str = f"${top1['price']:,.0f}" if top1["price"] else "N/A"

        # Calculate price match percentage
        target = data["profile"]["avg_price"]
        if top1["price"] and target:
            pct = abs(top1["price"] - target) / target * 100
            price_match = f"Within {pct:.0f}% of target"
        else:
            price_match = "N/A"

        target_beds = int(data["profile"]["avg_beds"])
        beds_match = "✅" if top1["beds"] == target_beds else "❌"

        target_types = list(data["profile"]["property_types"].keys())
        type_match = "✅" if top1["type"] in target_types else "❌"

        # Format Top-1 string
        # If sale, show in $1.85M if possible, if rent show as $4,600
        is_rent = "Rent" in data["goal"] or "rent" in data["goal"]
        if is_rent:
            t1_str = f"{top1['title']} (${top1['price']:,.0f})"
        else:
            if top1["price"] >= 1_000_000:
                t1_str = f"{top1['title']} (${top1['price']/1_000_000:.2f}M)"
            else:
                t1_str = f"{top1['title']} (${top1['price']:,.0f})"

        findings_rows.append((
            short, f"{ev['ndcg_5']:.3f}",
            t1_str,
            price_match, f"{type_match} {top1['type'][:6]}",
            f"{beds_match} {top1['beds']}BR"
        ))

    md_table(["Profile", "NDCG@5", "Top-1 Result", "Price Match", "Type", "Beds"], findings_rows)

    return all_eval


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="FYP Chapter 5 Evaluation Lab")
    parser.add_argument("--part", type=int, choices=[1, 2, 3], help="Run specific part only")
    parser.add_argument("--no-db", action="store_true", help="Skip DB, use cached CSV")
    args = parser.parse_args()

    header = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║            FYP CHAPTER 5 — COMPLETE EVALUATION LAB                         ║
║            Full-Stack Real Estate Research Platform                         ║
║                                                                            ║
║            Generating authentic evaluation evidence for:                   ║
║              • Valuation Model Training & Comparison                       ║
║              • Recommendation Engine Synthetic User Testing                ║
║              • All Chapter 5 Tables (5.1 – 5.9)                            ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
    emit(header)
    _md.append("# Chapter 5: Evaluation Lab Results (Auto-Generated)\n")
    _md.append(f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n")
    _md.append(f"_Evaluation seed: {RANDOM_STATE}  |  Split: {int((1-TEST_SIZE)*100)}/{int(TEST_SIZE*100)}  |  CV: {CV_FOLDS}-fold_\n")
    _md.append("---\n")

    emit(f"{_ts()} Evaluation Lab starting...")
    emit(f"{_ts()} Parts to run: {'All' if not args.part else f'Part {args.part} only'}")
    emit(f"{_ts()} Data source: {'Cached CSV' if args.no_db else 'PostgreSQL'}")
    emit("")

    # Load data (needed for Parts 1 & 2)
    df, engine = None, None
    if not args.part or args.part in [1, 2]:
        df, engine = load_data(use_db=not args.no_db)

    # Run parts
    if not args.part or args.part == 1:
        run_part1(df, engine)

    if not args.part or args.part == 2:
        run_part2(df)

    if not args.part or args.part == 3:
        run_part3()

    # Save results
    elapsed = round(time.time() - _start, 1)
    emit(f"\n{'='*70}")
    emit(f"{_ts()} Evaluation Lab complete. Total time: {elapsed}s")

    output_path = OUTPUT_DIR / "evaluation_lab_results.md"
    output_path.write_text("\n".join(_md))
    emit(f"{_ts()} Results saved to: {output_path}")
    emit(f"{'='*70}")


if __name__ == "__main__":
    main()
