# 🏠 Singapore Real Estate Data Platform

A complete data collection, processing, and visualization platform for Singapore real estate, featuring multi-platform scrapers, data pipelines, backend API, and frontend interface.

**[中文版 README](README_CN.md)**

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup Guide](#detailed-setup-guide)
  - [1. Clone and Install Dependencies](#1-clone-and-install-dependencies)
  - [2. Run Scrapers](#2-run-scrapers)
  - [3. Aggregate Data](#3-aggregate-data)
  - [4. Prepare External Data](#4-prepare-external-data)
  - [5. Start Backend Services](#5-start-backend-services)
  - [6. Ingest Data to PostgreSQL](#6-ingest-data-to-postgresql)
  - [7. Start Frontend](#7-start-frontend)
- [Project Structure](#project-structure)
- [FAQ](#faq)

---

## Overview

This project provides a complete Singapore real estate data solution:

- **Four Platform Scrapers**: PropertyGuru, 99.co, EdgeProp, SRX
- **Data Pipeline**: Aggregate multi-platform data, clean and standardize
- **Backend API**: FastAPI + PostgreSQL + Redis
- **Frontend Interface**: Next.js + TypeScript + TailwindCSS

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
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Backend API (backend/)                       │
│                 FastAPI + PostgreSQL + Redis                     │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend UI (frontend/)                       │
│                  Next.js + TypeScript + Leaflet                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

- **Python**: 3.10+
- **Node.js**: 18+
- **Docker & Docker Compose** (recommended for backend)
- **Browser Automation**: Playwright Chromium

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

# 3. Start backend (Docker)
cd backend && docker-compose up -d && cd ..

# 4. Start frontend
cd frontend && npm install && npm run dev
```

---

## Detailed Setup Guide

### 1. Clone and Install Dependencies

```bash
# Clone repository
git clone <repository-url>
cd PythonProject

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Install Python dependencies
pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

---

### 2. Run Scrapers

Scrapers save data to the `data/` directory (ignored in `.gitignore`).

#### PropertyGuru (Recommended First)

```bash
cd propertyguru

# Full scrape (initial setup)
python run_full.py

# Daily incremental update
python run_daily.py

# Cleanup expired listings
python run_cleanup.py
```

See [propertyguru/README.md](propertyguru/README.md) for details.

#### 99.co

```bash
cd 99co
python data_scraper_99co.py --purpose both --max-pages 50 --headless
```

See [99co/README.md](99co/README.md) for details.

#### EdgeProp

```bash
cd edgeprop
python edgeprop_scraper_v1.py --purpose sale --type condo --max-pages 50 --headless
python edgeprop_scraper_v1.py --purpose sale --type hdb --max-pages 50 --headless
python edgeprop_scraper_v1.py --purpose rental --type condo --max-pages 50 --headless
```

See [edgeprop/README.md](edgeprop/README.md) for details.

#### SRX

```bash
cd srx
python srx_data_scraper_6.py --purpose both --towns "1-28" --concurrency 6 --headless
```

See [srx/README.md](srx/README.md) for details.

---

### 3. Aggregate Data

After running scrapers, aggregate all platform data into a unified format.

```bash
cd pipeline
python aggregate.py
```

**Output Files:**

- `data/aggregated.db` - SQLite database
- `data/aggregated_listings.csv` - CSV format (for backup/debugging)

See [pipeline/README.md](pipeline/README.md) for details.

---

### 4. Prepare External Data

> [!IMPORTANT]
> The following data is **NOT** collected by scrapers and must be manually downloaded and placed in the correct location.

#### Agent Details (`agent_list.csv`)

This file contains agent details from CEA (Council for Estate Agencies), including:

- CEA registration number
- Company name
- License information
- Agent photo URL

**Download Link:** [Contact project maintainer for link]

**Location:**

```
data/
└── own/
    └── agent_list.csv
```

**File Format:**

```csv
id,cea_number,agent_name,phone,company_name,agency_license,license_expiry,registration_date,photo_url,created_at,updated_at
```

---

### 5. Start Backend Services

The backend uses Docker Compose to manage PostgreSQL and Redis.

```bash
cd backend

# Start all services (PostgreSQL, Redis, Backend)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

**Service Ports:**
| Service | Port |
|---------|------|
| Backend API | http://localhost:8000 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

**API Documentation:** http://localhost:8000/docs

#### Run Backend Locally (without Docker container)

```bash
# Start only PostgreSQL and Redis
docker-compose up -d db redis

# Run backend locally
pip install -r requirements.txt
uvicorn app.main:app --reload
```

---

### 6. Ingest Data to PostgreSQL

Aggregated data needs to be imported into PostgreSQL for the backend API.

```bash
cd pipeline

# Ensure backend database is running
# docker-compose up -d db  (in backend directory)

# Import aggregated data and agent data to PostgreSQL
python ingest.py
```

**This script will:**

1. Read listing data from `data/aggregated.db`
2. Read agent information from `data/own/agent_list.csv`
3. Create/update PostgreSQL tables:
   - `listings` - Property listings
   - `agents` - Agent information
   - `condo_basic` - Property basic info
   - `users` - User accounts

---

### 7. Start Frontend

```bash
cd frontend

# Install dependencies
npm install

# Development mode
npm run dev
```

Visit http://localhost:3000 to view the frontend.

#### Environment Variables

Create `.env.local` file in `frontend/` directory:

```env
NEXT_PUBLIC_API_URL=http://localhost:8000
```

---

## Project Structure

```
PythonProject/
├── 99co/                   # 99.co scraper
├── edgeprop/               # EdgeProp scraper
├── propertyguru/           # PropertyGuru scraper (most complete)
├── srx/                    # SRX scraper
├── pipeline/               # Data pipeline
│   ├── aggregate.py        # Aggregate multi-platform data
│   ├── ingest.py           # Import to PostgreSQL
│   └── README.md
├── backend/                # Backend API (FastAPI)
│   ├── app/
│   ├── docker-compose.yml
│   └── README.md
├── frontend/               # Frontend UI (Next.js)
│   ├── app/
│   ├── components/
│   └── README.md
├── data/                   # Data directory (gitignored)
│   ├── own/                # External data (manual placement)
│   │   └── agent_list.csv
│   └── aggregated.db
├── requirements.txt
├── README.md               # This file (English)
└── README_CN.md            # Chinese version
```

---

## FAQ

### Q: Do scrapers need API keys?

**PropertyGuru** requires proxy and API configuration (edit `propertyguru/config.py`). Other scrapers use Playwright to simulate browsers and don't need API keys.

### Q: How long does data collection take?

- **PropertyGuru full**: 2-4 hours
- **99.co**: 30-60 minutes
- **EdgeProp**: 30-60 minutes
- **SRX**: 1-2 hours

Use `--headless` flag to run in background.

### Q: How to update data from only some platforms?

Run the corresponding scraper, then re-run `pipeline/aggregate.py` to merge the latest data.

### Q: Backend fails to start?

```bash
docker-compose ps          # Check status
docker-compose logs backend # View logs
docker-compose down && docker-compose up -d --build  # Rebuild
```

### Q: Frontend can't connect to backend?

1. Confirm backend is running at http://localhost:8000
2. Check `NEXT_PUBLIC_API_URL` in `frontend/.env.local`
3. Verify CORS configuration in `backend/app/main.py`

---

## 📄 License

MIT License
