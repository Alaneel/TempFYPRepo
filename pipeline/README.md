# Data Pipeline

This directory contains scripts for aggregating and ingesting real estate data from multiple scrapers into the application database.

## Scripts

| Script                 | Description                                                      |
| ---------------------- | ---------------------------------------------------------------- |
| `aggregate.py`         | Aggregates data from all scrapers into a unified SQLite database |
| `ingest.py`            | Ingests aggregated data into PostgreSQL for the backend API      |
| `ingest_agent_list.py` | Imports external agent data from CSV                             |
| `db_init.py`           | Database initialization utilities                                |
| `export_db.py`         | Export data utilities                                            |

## Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│   Scraper Outputs (data/propertyguru/, data/99co/, etc.)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│   aggregate.py                                                   │
│   - Reads CSV/SQLite from each scraper                          │
│   - Normalizes schema (price, sqft, property_type, etc.)        │
│   - Deduplicates listings                                        │
│   - Outputs: data/aggregated.db, data/aggregated_listings.csv   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│   ingest.py                                                      │
│   - Reads from aggregated.db                                     │
│   - Reads agent_list.csv from data/own/                         │
│   - Creates/updates PostgreSQL tables (listings, agents, etc.)  │
└─────────────────────────────────────────────────────────────────┘
```

## Usage

### Step 1: Aggregate Data

Run after scraping to combine all platform data:

```bash
python aggregate.py
```

**Options:**

```bash
python aggregate.py --csv    # Also output CSV (for debugging)
python aggregate.py --help   # Show all options
```

**Output:**

- `../data/aggregated.db` - SQLite database with unified schema
- `../data/aggregated_listings.csv` - CSV export (optional)

### Step 2: Ingest to PostgreSQL

Before running, ensure PostgreSQL is running (via Docker):

```bash
# In backend/ directory
docker-compose up -d db

# Back in pipeline/ directory
python ingest.py
```

**Environment Variables:**

```bash
DB_HOST=localhost   # Default: localhost
DB_PORT=5432        # Default: 5432
DB_USER=postgres    # Default: postgres
DB_PASS=postgres    # Default: postgres
DB_NAME=real_estate_app  # Default: real_estate_app
```

## Unified Schema

The aggregated data follows this schema:

| Field           | Type   | Description                                 |
| --------------- | ------ | ------------------------------------------- |
| `source`        | string | Platform: propertyguru, 99co, edgeprop, srx |
| `source_id`     | string | Original listing ID from source             |
| `buy_rent`      | string | "Sale" or "Rent"                            |
| `title`         | string | Listing title                               |
| `address`       | string | Property address                            |
| `price`         | float  | Numeric price                               |
| `display_price` | string | Formatted price display                     |
| `price_psf`     | float  | Price per square foot                       |
| `display_psf`   | string | Formatted PSF display                       |
| `sqft`          | float  | Floor area in sqft                          |
| `beds`          | int    | Number of bedrooms                          |
| `baths`         | int    | Number of bathrooms                         |
| `property_type` | string | Condo, HDB, Landed, etc.                    |
| `tenure`        | string | Freehold, Leasehold, etc.                   |
| `district`      | int    | Singapore district number                   |
| `url`           | string | Original listing URL                        |
| `image_url`     | string | Main listing image                          |
| `agent_name`    | string | Agent name                                  |
| `agent_cea`     | string | Agent CEA number                            |
| `agent_phone`   | string | Agent contact                               |
