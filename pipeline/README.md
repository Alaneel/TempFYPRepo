# Data Pipeline

Scripts for aggregating, ingesting, and ML-modelling Singapore real estate data.

## Scripts

| Script                 | Description                                                            |
| ---------------------- | ---------------------------------------------------------------------- |
| `aggregate.py`         | Merges all scraper outputs into a unified SQLite database              |
| `ingest.py`            | Ingests aggregated data into PostgreSQL for the backend API            |
| `valuation_model.py`   | Trains 8 per-type property valuation models with SHAP interpretability |
| `ingest_agent_list.py` | Imports external agent data from CSV                                   |
| `db_init.py`           | Database initialization utilities                                      |

---

## Workflow

```
Scrapers (propertyguru/, 99co/, edgeprop/, srx/)
        │
        ▼
aggregate.py  →  data/aggregated.db  →  ingest.py  →  PostgreSQL
                                                            │
                                              valuation_model.py
                                                            │
                                              models/valuation/
                                              (8 pkl models + metrics.json)
```

---

## Usage

### Step 1: Aggregate

```bash
python aggregate.py          # Normal run
python aggregate.py --csv    # Also output CSV
```

Output: `../data/aggregated.db`

### Step 2: Ingest to PostgreSQL

```bash
# Backend DB must be running
python ingest.py
```

### Step 3: Train Valuation Models

```bash
# Full training — ~3 min, trains 8 models
python valuation_model.py

# Quick mode — ~20 sec (skip LightGBM, skip local SHAP waterfall)
python valuation_model.py --quick

# Use cached CSV (skip DB connection)
python valuation_model.py --no-db --quick
```

**Output:** `../models/valuation/` (gitignored — each developer trains locally)

```
models/valuation/
├── condo_sale/     best_model.pkl  metrics.json  residuals.png  shap_bar.png
├── condo_rent/     best_model.pkl  metrics.json  ...
├── hdb_sale/       ...
├── hdb_rent/       ...
├── landed_sale/    ...
├── landed_rent/    ...
├── gcb_sale/       ...
├── gcb_rent/       ...
└── evaluation_report.html
```

---

## Valuation Model Details

Separate models per `(property_type × sale/rent)` — 8 models total.
**Property_type is NOT a feature** — each model is already pure to one type, which prevents HDB rent data from dragging down Condo rent predictions.

### Features

| Feature         | Description                        |
| --------------- | ---------------------------------- |
| `beds`          | Number of bedrooms                 |
| `sqft`          | Floor area (sqft)                  |
| `log_sqft`      | log₁₀(sqft)                        |
| `beds_sqft`     | Interaction: beds × sqft           |
| `beds_sq`       | beds²                              |
| `log_beds_sqft` | beds × log₁₀(sqft)                 |
| `sqft_bin`      | Quantile-binned size tier (0–4)    |
| `is_freehold`   | 1 = Freehold tenure, 0 = Leasehold |

### Target

`log₁₀(price)` — log transformation stabilises variance and handles right-skewed price distributions.

### Model Selection (per segment)

Trains: Baseline → Ridge → Random Forest → XGBoost (→ LightGBM in full mode)  
Saves the XGBoost model as default best (unless not available, falls back to RF → Ridge).

### Accuracy (Quick Mode, approx.)

| Segment     | MAPE  | R²   |
| ----------- | ----- | ---- |
| condo_sale  | 22.7% | 0.81 |
| condo_rent  | 19.0% | 0.90 |
| hdb_sale    | 14.7% | 0.56 |
| hdb_rent    | 18.1% | 0.86 |
| landed_sale | 26.2% | 0.61 |
| landed_rent | 24.6% | 0.90 |
| gcb_sale    | 25.6% | 0.21 |
| gcb_rent    | 21.0% | 0.42 |

> GCB accuracy is lower due to small sample sizes (~300–450) and high location dependence which cannot be captured without geographic data.

---

## Unified Schema (aggregated.db)

| Field           | Type   | Description                                      |
| --------------- | ------ | ------------------------------------------------ |
| `source`        | string | Platform: propertyguru, 99co, edgeprop, srx      |
| `source_id`     | string | Original listing ID from source                  |
| `buy_rent`      | string | `property-for-sale` or `property-for-rent`       |
| `title`         | string | Listing title                                    |
| `price`         | float  | Numeric price (SGD)                              |
| `psf`           | float  | Price per square foot                            |
| `sqft`          | float  | Floor area (sqft)                                |
| `beds`          | int    | Bedrooms                                         |
| `baths`         | int    | Bathrooms                                        |
| `property_type` | string | Condominium / HDB / Landed / Good Class Bungalow |
| `tenure`        | string | Freehold / Leasehold 99 / etc.                   |
| `district`      | int    | Singapore district number                        |
| `url`           | string | Original listing URL                             |
| `agent_cea`     | string | Agent CEA registration number                    |
