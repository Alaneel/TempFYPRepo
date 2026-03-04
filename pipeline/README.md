# Data Pipeline

Scripts for aggregating, geocoding, enriching, and ML-modelling Singapore real estate data.

---

## 🗺 Script Overview

| Script | When to run | Description |
|---|---|---|
| `aggregate.py` | After scraping | Merge all scraper outputs → `data/aggregated.db` |
| `ingest.py` | After aggregate | Load aggregated data → PostgreSQL |
| `ingest_agent_list.py` | Once | Import agent CSV into PostgreSQL |
| `geocode_listings.py` | After ingest | Forward geocode addresses → lat/lng (OneMap) |
| `reverse_geocode_district.py` | After geocode | lat/lng → postal code → district (OneMap) |
| `valuation_model.py` | After district data ready | Train 8 ML models, save to `models/valuation/` |
| `refresh_onemap_token.py` | Every ~3 days | Auto-renew OneMap API token → writes to `.env` |
| `export_db.py` | On demand | Export PostgreSQL tables to CSV |
| `db_init.py` | Once (setup) | Database schema initialization utilities |
| `eval_dedup.py` | On demand | Evaluate listing deduplication quality |
| `eval_semantic_search.py` | On demand | Evaluate semantic search accuracy |

---

## 🔄 Standard Workflow (First-Time Setup)

```
1. Run scrapers (propertyguru/, 99co/, edgeprop/, srx/)
        │
        ▼
2. aggregate.py  ──→  data/aggregated.db
        │
        ▼
3. ingest.py  ──→  PostgreSQL (listings table)
        │
        ├──→ ingest_agent_list.py  (agents table, run once)
        │
        ▼
4. geocode_listings.py  ──→  listings.latitude / longitude
        │
        ▼
5. reverse_geocode_district.py  ──→  listings.district
        │
        ▼
6. valuation_model.py  ──→  models/valuation/ (8 models)
```

Steps 4–5 require a valid OneMap token in `.env`.
Run `python refresh_onemap_token.py` first if you don't have one.

---

## Usage

### Step 1 — Aggregate scraper outputs

```bash
python aggregate.py          # Normal run
python aggregate.py --csv    # Also output CSV
```

Output: `../data/aggregated.db`

---

### Step 2 — Ingest to PostgreSQL

Backend DB must be running first.

```bash
python ingest.py
```

---

### Step 3 — Ingest agent list (once)

```bash
python ingest_agent_list.py
```

Requires: `data/own/agent_list.csv`

---

### Step 4 — Geocode listings (address → lat/lng)

```bash
python geocode_listings.py
```

Calls OneMap forward search API. Requires `ONEMAP_TOKEN` in `.env`.

---

### Step 5 — Reverse geocode district (lat/lng → district)

```bash
python reverse_geocode_district.py          # Fill only NULL district (safe to re-run)
python reverse_geocode_district.py --limit 100   # Test with 100 rows first
python reverse_geocode_district.py --overwrite   # Overwrite all (after new data)
```

**Token management:** OneMap tokens expire every ~3 days. Auto-refresh:

```bash
python refresh_onemap_token.py
```

Requires `ONEMAP_EMAIL` and `ONEMAP_PASSWORD` in `.env`.

---

### Step 6 — Train valuation models

```bash
# Full training — ~3 min, trains 8 LightGBM models (recommended)
python valuation_model.py

# Quick mode — ~20 sec, skips LightGBM
python valuation_model.py --quick

# Use cached CSV (skip DB query, useful for iteration)
python valuation_model.py --no-db
```

> **Note:** Pre-trained models are already in the repo (`models/valuation/*.pkl`).  
> Only retrain after adding new data or changing features.

**Output:**
```
models/valuation/
├── condo_sale/    best_model.pkl  metrics.json  shap_bar.png  residuals.png
├── condo_rent/    ...
├── hdb_sale/      ...
├── hdb_rent/      ...
├── landed_sale/   ...
├── landed_rent/   ...
├── gcb_sale/      ...
├── gcb_rent/      ...
├── eda/           eda_overview.png
└── evaluation_report.html
```

---

## Valuation Model Details

8 separate models — one per `(property_type × sale/rent)` combination.  
Property type is NOT a feature — each model is trained on data for one type only.

### Features

| Feature | Description |
|---|---|
| `beds` | Number of bedrooms |
| `sqft` | Floor area (sqft) |
| `log_sqft` | log₁₀(sqft) |
| `beds_sqft` | Interaction: beds × sqft |
| `beds_sq` | beds² |
| `log_beds_sqft` | beds × log₁₀(sqft) |
| `sqft_bin` | Quantile-binned size tier (0–4) |
| `is_freehold` | 1 = Freehold, 0 = Leasehold |
| `property_age` | 2026 − built_year |
| `district` | Singapore district 1–28 (from reverse geocoding) |

Missing values are filled with per-segment medians before training.

### Target

`log₁₀(price)` — log transformation stabilises variance across price ranges.

### Model Selection

Trains Baseline → Ridge → Random Forest → XGBoost → LightGBM per segment.  
Selects best by cross-validated R². LightGBM wins in most segments.

### Current Accuracy (Full Mode, LightGBM)

| Segment | MAPE | R² |
|---|---|---|
| condo_sale | 11.2% | 0.945 |
| condo_rent | 9.8% | 0.933 |
| hdb_sale | 7.2% | 0.897 |
| hdb_rent | 9.3% | 0.798 |
| landed_sale | 24.8% | 0.627 |
| landed_rent | 25.2% | 0.786 |
| gcb_sale | 22.0% | 0.376 |
| gcb_rent | 21.8% | 0.513 |

> GCB and Landed accuracy is lower due to high location dependence and small sample sizes.  
> The `district` feature significantly improved condo/HDB accuracy vs previous version.

---

## Unified Schema (aggregated.db / PostgreSQL listings table)

| Field | Type | Description |
|---|---|---|
| `source` | string | Platform: propertyguru / 99co / edgeprop / srx |
| `source_id` | string | Original listing ID |
| `buy_rent` | string | `property-for-sale` or `property-for-rent` |
| `title` | string | Listing title |
| `price` | float | Numeric price (SGD) |
| `psf` | float | Price per square foot |
| `sqft` | float | Floor area (sqft) |
| `beds` | int | Bedrooms |
| `baths` | int | Bathrooms |
| `property_type` | string | Condominium / HDB / Landed / Good Class Bungalow |
| `tenure` | string | Freehold / Leasehold 99 / etc. |
| `built_year` | int | Year built |
| `district` | int | Singapore district 1–28 |
| `latitude` | float | From geocode_listings.py |
| `longitude` | float | From geocode_listings.py |
| `address` | string | Street address |
| `url` | string | Original listing URL |
| `agent_cea` | string | Agent CEA registration number |
