# 🏠 Singapore Real Estate Data Platform

A complete data collection, processing, and AI analysis platform for Singapore real estate — featuring multi-platform scrapers, data pipeline, FastAPI backend, Next.js frontend, **semantic search**, and **AI-powered property valuation**.

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
- **Semantic Search**: Natural language property search powered by Claude AI
- **AI Valuation**: Per-property-type price estimation with SHAP interpretability

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
│  valuation_model.py → 8 per-type ML models (models/valuation/)  │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Backend API (backend/)                       │
│   FastAPI + PostgreSQL + Redis                                   │
│   /api/v1/listings   — browse & filter                          │
│   /api/v1/listings/semantic-search  — Claude AI NL search       │
│   /api/v1/valuation/estimate        — AI price estimation        │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend UI (frontend/)                        │
│   Next.js + TypeScript + Leaflet + TailwindCSS                   │
│   /listings   — browse with AI Search toggle                     │
│   /listings/[id]  — detail with AI Valuation panel              │
│   /valuate    — standalone property valuation tool               │
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
> Models are gitignored (large binary files). Each team member must train locally.

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
- Or use the **AI Search** button on the homepage hero
- Claude parses intent → structured filters → listings query
- Parsed filters shown as tags below the search bar

**API:** `POST /api/v1/listings/semantic-search`

### 🏷 AI Valuation

Per-property-type price estimation using XGBoost/RF models trained on 50K+ listings.

| Model       | Accuracy (MAPE) | R²   |
| ----------- | --------------- | ---- |
| Condo Sale  | 22.7%           | 0.81 |
| Condo Rent  | 19.0%           | 0.90 |
| HDB Sale    | 14.7%           | 0.56 |
| HDB Rent    | 18.1%           | 0.86 |
| Landed Sale | 26.2%           | 0.61 |
| Landed Rent | 24.6%           | 0.90 |

**Two usage points:**

1. **Listing detail page** — AI Valuation panel in right sidebar shows estimate vs listed price (over/under-priced badge) + SHAP attribution
2. **`/valuate` page** — Standalone estimator with form inputs

**API:** `POST /api/v1/valuation/estimate`

---

## Project Structure

```
PythonProject/
├── 99co/                   # 99.co scraper
├── edgeprop/               # EdgeProp scraper
├── propertyguru/           # PropertyGuru scraper
├── srx/                    # SRX scraper
├── pipeline/               # Data pipeline
│   ├── aggregate.py        # Merge multi-platform scraper data
│   ├── ingest.py           # Import to PostgreSQL
│   ├── valuation_model.py  # ML training pipeline (8 models)
│   └── README.md
├── backend/                # FastAPI backend
│   ├── app/
│   │   ├── routers/
│   │   │   ├── listings.py     # Listings + semantic search
│   │   │   ├── valuation.py    # AI valuation API  ← NEW
│   │   │   ├── agents.py
│   │   │   └── auth.py
│   │   └── services/
│   │       ├── valuation.py    # Model loader + SHAP  ← NEW
│   │       └── ...
│   └── README.md
├── frontend/               # Next.js frontend
│   ├── app/
│   │   ├── listings/
│   │   │   ├── page.tsx         # Listings with AI Search toggle
│   │   │   └── [id]/page.tsx    # Detail with AI Valuation panel
│   │   ├── valuate/
│   │   │   └── page.tsx         # Standalone valuator  ← NEW
│   │   └── page.tsx             # Homepage with AI Search button
│   └── components/
│       └── features/listings/
│           └── valuation-panel.tsx  ← NEW
├── models/                 # Trained models (gitignored, local only)
│   └── .gitkeep
├── data/                   # Scraped data (gitignored)
│   └── own/                # External data (manual placement)
├── .env.example            # Environment variable template
├── requirements.txt        # Python dependencies
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
