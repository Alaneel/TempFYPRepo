# 🏠 SingaLiving — Singapore Real Estate AI Platform

A complete data collection, processing, and AI analysis platform for Singapore real estate — featuring multi-platform scrapers, data pipeline, FastAPI backend, Next.js frontend, **semantic search**, **AI-powered property valuation**, and **personalised property recommendations**.

**[中文版 README](README_CN.md)**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup Guide](#detailed-setup-guide)
  - [1. Clone and Install Dependencies](#1-clone-and-install-dependencies)
  - [2. Configure Environment Variables](#2-configure-environment-variables)
  - [3. Run Scrapers](#3-run-scrapers)
  - [4. Aggregate Data](#4-aggregate-data)
  - [5. Prepare External Data](#5-prepare-external-data)
  - [6. Start Backend Services](#6-start-backend-services)
  - [7. Ingest Data to PostgreSQL](#7-ingest-data-to-postgresql)
  - [8. Train Valuation Models](#8-train-valuation-models)
  - [9. Start Frontend](#9-start-frontend)
- [AI Features](#ai-features)
- [Project Structure](#project-structure)
- [FAQ](#faq)

---

## Overview

This project provides a complete Singapore real estate data and AI platform:

- **Four Platform Scrapers**: PropertyGuru, 99.co, EdgeProp, SRX
- **Data Pipeline**: Aggregate, clean and standardize multi-platform data
- **Backend API**: FastAPI + PostgreSQL + Redis
- **Frontend UI**: Next.js + TypeScript + TailwindCSS
- **Semantic Search**: Natural language property search powered by Claude AI, with agentic enhancements (multi-district resolution, progressive filter relaxation, fallback explanations)
- **AI Valuation**: Per-segment XGBoost price estimation with SHAP interpretability and a multi-turn chat assistant
- **Personalised Recommendations**: Hybrid content-based + valuation-grounded recommendation engine (NDCG@5 = 0.811)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Collection Layer                       │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│ PropertyGuru│    99.co    │  EdgeProp   │     SRX     │ External│
│  (Scraper)  │  (Scraper)  │  (Scraper)  │  (Scraper)  │  Data   │
└──────┬──────┴──────┬──────┴──────┬──────┴──────┬──────┴────┬────┘
       │             │             │             │           │
       ▼             ▼             ▼             ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Data Pipeline (pipeline/)                      │
│  aggregate.py → aggregated.db → ingest.py → PostgreSQL          │
│  valuation_model.py → 8 per-segment XGBoost models              │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Backend API (backend/)                       │
│   FastAPI + PostgreSQL + Redis                                   │
│   /api/v1/listings            — browse & filter                  │
│   /api/v1/listings/semantic-search  — Claude NL search + agents │
│   /api/v1/valuation/estimate        — AI price estimation        │
│   /api/v1/listings/{id}/chat        — valuation chat assistant   │
│   /api/v1/recommendations           — personalised listings      │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend UI (frontend/)                        │
│   Next.js + TypeScript + Leaflet + TailwindCSS                   │
│   /listings        — browse with AI Search toggle                │
│   /listings/[id]   — detail with AI Valuation panel + chat       │
│   /saved           — saved listings                              │
│   /for-you         — personalised recommendations                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- **Python**: 3.10+
- **Node.js**: 18+
- **Docker & Docker Compose** (recommended for backend)
- **Browser Automation**: Playwright Chromium
- **Anthropic API key** (for semantic search — optional)

---

## Quick Start

```bash
# 1. Clone repository
git clone <repository-url>
cd PythonProject

# 2. Install Python dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# 3. Configure environment
cp .env.example .env   # edit with your DB credentials and API keys

# 4. Start backend (Docker)
cd backend && docker-compose up -d && cd ..

# 5. Start frontend
cd frontend && npm install && npm run dev
```

---

## Detailed Setup Guide

### 1. Clone and Install Dependencies

```bash
git clone <repository-url>
cd PythonProject

python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

pip install -r requirements.txt
playwright install chromium
```

---

### 2. Configure Environment Variables

Create a `.env` file in the project root (it is gitignored):

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Database (must match backend/docker-compose.yml)
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_db_user
DB_PASS=your_db_password
DB_NAME=real_estate_app

# Semantic Search (optional — only needed for AI Search feature)
ANTHROPIC_API_KEY=sk-ant-...

# OneMap (for district reverse geocoding — run pipeline/refresh_onemap_token.py to auto-refresh)
ONEMAP_EMAIL=your_onemap_email
ONEMAP_PASSWORD=your_onemap_password
ONEMAP_TOKEN=                      # auto-filled by refresh_onemap_token.py

# Backend settings
SECRET_KEY=your-random-secret-key
```

Frontend environment (`frontend/.env.local`):

```env
NEXT_PUBLIC_API_URL=http://localhost:8000/api/v1
```

---

### 3. Run Scrapers

Scrapers save data to the `data/` directory (gitignored).

#### PropertyGuru

```bash
cd propertyguru
python run_full.py     # Full scrape
python run_daily.py    # Daily incremental
```

#### 99.co

```bash
cd 99co
python data_scraper_99co.py --purpose both --max-pages 50 --headless
```

#### EdgeProp

```bash
cd edgeprop
python edgeprop_scraper_v1.py --purpose sale --type condo --max-pages 50 --headless
python edgeprop_scraper_v1.py --purpose rental --type condo --max-pages 50 --headless
```

#### SRX

```bash
cd srx
python srx_data_scraper_6.py --purpose both --towns "1-28" --concurrency 6 --headless
```

---

### 4. Aggregate Data

```bash
python pipeline/aggregate.py
```

Output: `data/aggregated.db` (SQLite, gitignored)

---

### 5. Prepare External Data

> [!IMPORTANT]
> The following files are **NOT** collected by scrapers and must be manually placed.

#### Agent List (`agent_list.csv`)

CEA agent data including registration number, company, photo URL.

```
data/own/agent_list.csv
```

#### Condo Reference Data (`property_basic.csv`)

Contains condo geographic coordinates and project metadata.

```
data/own/property_basic.csv
```

---

### 6. Start Backend Services

```bash
cd backend
docker-compose up -d
```

| Service     | Port | Description         |
| ----------- | ---- | ------------------- |
| Backend API | 8000 | FastAPI application |
| PostgreSQL  | 5432 | Primary database    |
| Redis       | 6379 | Cache layer         |

**Local development (without Docker container for backend):**

```bash
cd backend
docker-compose up -d db redis   # just DB + Redis
uvicorn app.main:app --reload   # run backend locally
```

---

### 7. Ingest Data to PostgreSQL

```bash
cd pipeline
python ingest.py
```

Imports listings, agents, and condo reference data into PostgreSQL.

---

### 8. Train Valuation Models

> [!NOTE]
> Models are included in the repository (`*.pkl`, ~8MB total). No training needed after cloning — the valuation API works immediately.
> To retrain (e.g. after new data):

```bash
# Full training — 8 models (Condo/HDB/Landed/GCB × sale/rent) ~3 min
python pipeline/valuation_model.py

# Quick mode (skip LightGBM + local SHAP) ~20 sec
python pipeline/valuation_model.py --quick

# Use cached data (skip DB query)
python pipeline/valuation_model.py --no-db --quick
```

After training, models are saved to `models/valuation/` and the valuation API (`/api/v1/valuation/estimate`) becomes available automatically (lazy-loaded on first request).

---

### 9. Start Frontend

```bash
cd frontend
npm install
npm run dev
```

Visit **http://localhost:3000**

---

## AI Features

### 🔍 Semantic Search

Natural language property queries powered by Claude AI.

- On the listings page, toggle **AI Search** to enable
- Claude parses intent → structured filters → listings query
- Parsed filters shown as tags below the search bar

**Agentic enhancements:**
- **Multi-district resolution**: queries like "near Orchard MRT" resolve to districts [9, 10, 11]
- **Progressive filter relaxation**: zero-result queries auto-relax constraints (price → tenure → district)
- **Fallback explanations**: LLM-generated natural language explanation when filters are relaxed

**API:** `POST /api/v1/listings/semantic-search`

### 🏷 AI Valuation

Per-segment XGBoost price estimation trained on 53,497 deduplicated listings, with SHAP feature attribution.

| Model       | MAPE   | R²    |
| ----------- | ------ | ----- |
| Condo Sale  | 13.4%  | 0.923 |
| Condo Rent  | 14.2%  | 0.914 |
| HDB Sale    | 9.0%   | 0.888 |
| HDB Rent    | 8.9%   | 0.784 |
| Landed Sale | 26.3%  | 0.654 |
| Landed Rent | 26.2%  | 0.864 |
| GCB Sale    | 25.3%  | 0.072 |
| GCB Rent    | 19.8%  | 0.615 |

**Usage:**
1. **Listing detail page** — AI Valuation panel shows estimate vs listed price + SHAP attribution
2. **Chat assistant** — multi-turn Q&A about valuation, grounded in XGBoost + SHAP context (93% factual consistency across 42 offline interactions)

**API:** `POST /api/v1/valuation/estimate` | `POST /api/v1/listings/{id}/chat`

### ⭐ Personalised Recommendations

Hybrid content-based + valuation-grounded recommendation engine.

- Derives user preference profile from saved listings
- Ranks candidates by a five-component weighted score: property-type affinity, district affinity, price-band similarity, bedroom proximity, and **bargain score** (XGBoost estimate vs asking price)
- **Offline evaluation** across 10 synthetic profiles: NDCG@5 = 0.811, Precision@5 = 0.800, 100% type-match and district-match at rank 1

**API:** `GET /api/v1/recommendations`

---

## Project Structure

```
PythonProject/
├── 99co/                        # 99.co scraper
├── edgeprop/                    # EdgeProp scraper
├── propertyguru/                # PropertyGuru scraper
├── srx/                         # SRX scraper
├── pipeline/                    # Data pipeline
│   ├── aggregate.py                  # Merge multi-platform scraper data
│   ├── ingest.py                     # Import to PostgreSQL
│   ├── geocode_listings.py           # Forward geocode (address → lat/lng)
│   ├── reverse_geocode_district.py   # Reverse geocode (lat/lng → district)
│   ├── refresh_onemap_token.py       # Auto-refresh OneMap API token
│   ├── valuation_model.py            # ML training pipeline (8 XGBoost models)
│   ├── chat_eval.py                  # Offline chat assistant evaluation (42 interactions)
│   └── README.md
├── backend/                     # FastAPI backend
│   ├── app/
│   │   ├── routers/
│   │   │   ├── listings.py           # Listings + semantic search + chat
│   │   │   ├── valuation.py          # AI valuation API
│   │   │   ├── recommendations.py    # Personalised recommendation API
│   │   │   ├── agents.py
│   │   │   └── auth.py
│   │   └── services/
│   │       ├── valuation.py          # Model loader + SHAP
│   │       ├── recommendation.py     # Scoring function + NDCG evaluation
│   │       └── ...
│   └── README.md
├── frontend/                    # Next.js frontend
│   ├── app/
│   │   ├── listings/
│   │   │   ├── page.tsx              # Listings with AI Search toggle
│   │   │   └── [id]/page.tsx         # Detail with AI Valuation panel + chat
│   │   ├── saved/
│   │   │   └── page.tsx              # Saved listings
│   │   ├── for-you/
│   │   │   └── page.tsx              # Personalised recommendations
│   │   └── page.tsx                  # Homepage
│   └── components/
├── analysis_hdb_transacted/     # HDB transacted price model comparison (vs asking price)
├── eval_recommendation/         # Offline NDCG evaluation for recommendation engine
├── devtools/                    # Development utilities
│   └── ingest_sqlite.py              # Legacy SQLite ingestion tool
├── docs/                        # Developer documentation
│   └── cloudbypass_api_guide.md      # CloudBypass anti-bot API guide
├── models/                      # Trained models (gitignored, local only)
│   └── .gitkeep
├── data/                        # Scraped data (gitignored)
│   └── own/                          # External data (manual placement)
├── .env.example                 # Environment variable template
├── requirements.txt             # Python dependencies
└── README.md
```

---

## FAQ

**Q: Do scrapers need API keys?**

PropertyGuru may require proxy configuration (see `propertyguru/config.py`). Other scrapers use Playwright. Semantic search requires an `ANTHROPIC_API_KEY`.

**Q: How long does training take?**

`--quick` mode: ~20 seconds. Full training with LightGBM: ~3 minutes.

**Q: Valuation API returns 503?**

Models haven't been trained yet. Run `python pipeline/valuation_model.py --quick`.

**Q: Backend fails to start?**

```bash
docker-compose ps
docker-compose logs backend
docker-compose down && docker-compose up -d --build
```

**Q: Frontend can't connect to backend?**

1. Confirm backend is at http://localhost:8000
2. Check `NEXT_PUBLIC_API_URL` in `frontend/.env.local`
3. Verify CORS settings in `backend/app/main.py`

---

## 📄 License

MIT License
