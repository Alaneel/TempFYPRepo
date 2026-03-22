# Comprehensive Technical Specification: Real Estate AI Portal

This document provides a detailed technical overview of the Final Year Project (FYP). The system is a hybrid real estate analysis platform that combines web scraping, machine learning valuation, and an AI-driven recommendation engine.

---

## 1. System Architecture Overview

The application follows a modern decoupled architecture:
*   **Frontend**: React/Next.js SPA (Single Page Application) with a "Bento-style" dashboard.
*   **Backend**: FastAPI (Python) providing RESTful endpoints for search, valuation, and AI chat.
*   **Database**: PostgreSQL (Relational) for structured data and SQLite (Temporary) for aggregation.
*   **Data Pipeline**: Python-driven ETL (Extract, Transform, Load) for multi-source scraping.

---

## 2. Master Data & Enrichment Strategy

A core innovation of this project is the **Entity Resolution** system, which link raw, messy "listings" from the web to a clean, authoritative "Master Directory."

### Data Directory: `data/basic/`
| File | Scope | Entity Count | Matching Logic |
| :--- | :--- | :--- | :--- |
| `property_basic.csv` | Private Condos / ECs | 2,500 | Exact or substring match on **Project Name**. |
| `hdb_basic.csv` | HDB Blocks | 13,267 | Concatenated match on **Block + Street Name**. |
| `agent_list.csv` | Real Estate Agents | ~500 | Match on **Name + Mobile** to verify CEA status. |

### Enrichment Process
When `ingest.py` runs, it "blindly" takes a raw listing (e.g., from PropertyGuru) and attempts to find its match in the Master Directories. If a match is found, the listing is "enriched" with:
*   **Geospatial Data**: Accurate Latitude/Longitude for the Map view.
*   **Project Specs**: Year of completion, total units, and tenure.
*   **Facilities**: Boolean flags for `has_gym`, `has_pool`, `has_security`.

---

## 3. Machine Learning & Predictive Analytics

### Automated Valuation Model (AVM)
*   **Algorithm**: XGBoost (Extreme Gradient Boosting).
*   **Performance**: **18.0% MAPE** (Mean Absolute Percentage Error).
*   **Features**: Unit size, floor level, tenure, district, and distance to the nearest MRT.
*   **Explainability**: Uses **SHAP (SHapley Additive exPlanations)** to break down why the AI estimated a certain price.

### Hybrid Recommendation Engine
*   **Algorithm**: A combination of Content-Based Filtering and Collaborative User Profiling.
*   **Evaluation**: Achieved a mean **NDCG@5 score of 0.833**.
*   **Matching Factors**: Price-per-square-foot (PSF), property type consistency, and "Value Score" (Listing Price vs. AI Valuation).

---

## 4. Key Platform Features

### Interactive Mapping (Leaflet.js)
The portal uses Leaflet.js to visualize property density. Markers are dynamically filtered based on "For Sale" vs "For Rent" status. Coordinates are sourced from the `property_basic` master files.

### Property AI Assistant
A context-aware chatbot (powered by Claude/Haiku) that can:
1.  Analyze a property's "Value-for-Money" using the internal valuation model.
2.  Summarize property facilities and neighborhood amenities.
3.  Compare multiple properties based on user preferences.

### Bento UI Dashboard
The landing page uses a high-density "Bento" layout to show:
*   Latest high-value "Bargain" listings.
*   Personalized property recommendations.
*   Quick-access search by district or property type.

---

## 5. Directory & Script Reference

*   `pipeline/ingest.py`: The main database setup and master data enrichment script.
*   `pipeline/evaluation_lab.py`: The benchmarking suite for ML models (NDCG, MAPE).
*   `backend/app/services/valuation.py`: Live inference logic for the XGBoost model.
*   `data/aggregated.db`: The intermediate SQLite database that holds raw scraped listings Before they are cleaned and moved to Postgres.
*   `.env`: Configuration for Database credits, API keys, and model paths.
