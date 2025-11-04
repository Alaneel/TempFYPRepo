# PropertyGuru Scraper and Aggregator

## 📋 Project Overview

This project is a comprehensive system for scraping property information from PropertyGuru and storing it in a structured database. It consists of two main components:

1.  **`property_scraper`**: A Scrapy project for crawling and parsing property listings.
2.  **`property_aggregator`**: A set of tools for managing the aggregated data in a PostgreSQL database.

### ✨ Key Features

*   **Modern Scraping**: Utilizes Scrapy and Playwright for efficient and robust web scraping.
*   **Centralized Database**: Stores all data in a PostgreSQL database, providing a single source of truth.
*   **Incremental Updates**: Smart incremental update with early stopping mechanism.
*   **Data Management**: Includes tools for database initialization and maintenance.
*   **Clear Separation of Concerns**: The scraper and aggregator are separate modules, making the system easier to maintain and extend.

## 🏗️ Project Structure

```
/
├── property_scraper/       # Scrapy project for scraping
│   ├── property_scraper/
│   │   ├── spiders/
│   │   │   └── propertyguru_spider.py  # Main spider with incremental update support
│   │   ├── items.py
│   │   ├── pipelines.py              # Stores data in PostgreSQL
│   │   └── settings.py               # Scrapy settings
│   └── scrapy.cfg
│
├── property_aggregator/    # Tools for data aggregation and management
│   ├── create_tables.py          # Initializes the database schema
│   ├── database.py               # Database connection and session management
│   ├── mark_inactive_listings.py # Marks listings that are no longer active
│   ├── models.py                 # SQLAlchemy models for the database
│   ├── incremental_updater.py    # Incremental update management
│   └── spider_config.py          # Spider configuration manager
│
├── data/                     # Data directory (exports, etc.)
├── logs/                     # Log files
│
├── run_spider.py             # Auto-mode spider runner (recommended daily use)
├── run_full.py               # Full crawl script
├── run_expired.py            # Expired listings update script
├── manage_db.py              # Interactive database management tool
│
├── config.py                 # Main configuration file
├── config_example.py         # Example configuration file
├── requirements.txt          # Project dependencies
│
└── INCREMENTAL_UPDATE.md     # Detailed incremental update guide
```

## 🚀 Getting Started

### 1. Installation

Clone the repository and install the required dependencies:

```bash
git clone <repository_url>
cd <repository_directory>
pip install -r requirements.txt
```

### 2. Configuration

1.  **Copy the example configuration:**
    ```bash
    cp config_example.py config.py
    ```

2.  **Edit `config.py`:**
    Fill in your PostgreSQL connection string

3.  **Configure Scrapy Settings:**
    Edit `property_scraper/property_scraper/settings.py`:
    - `CLOUDBYPASS_APIKEY`: Your CloudBypass API key
    - `CLOUDBYPASS_PROXY`: Your proxy configuration

### 3. Database Initialization

```bash
python -m property_aggregator.create_tables
```

### 4. Running the Scraper

#### Option 1: Auto Mode (Recommended)
```bash
# Auto-selects FULL or INCREMENTAL based on last update
python run_spider.py
```

#### Option 2: Full Crawl
```bash
python run_full.py
```

#### Option 3: Update Expired Listings
```bash
python run_expired.py
```

#### Option 4: Direct Command
```bash
scrapy crawl propertyguru -a mode=INCREMENTAL
```

## 🔄 Update Modes

### FULL Mode
- Crawls all listings from scratch
- Use cases: First run, complete data refresh
- Duration: 6-12 hours

### INCREMENTAL Mode (Recommended)
- Smart incremental update with early stopping
- Automatically stops after encountering known listings
- Use cases: Daily maintenance
- Duration: 15-60 minutes

### EXPIRED Mode
- Updates listings not seen for 90+ days
- Use cases: Monthly maintenance, agent info update
- Duration: 1-3 hours

## 📊 Data Management

### Interactive Database Manager

```bash
python manage_db.py
```

Features:
1. View database statistics
2. Mark inactive listings (7+ days)
3. View expired listings (90+ days)
4. Check recent new listings (24h)
5. Check recent updates (24h)

### Programmatic Access

```python
from property_aggregator.incremental_updater import IncrementalUpdater

updater = IncrementalUpdater()
stats = updater.get_stats()
count = updater.mark_as_inactive(days_threshold=7)
```

## 🛠️ Database Maintenance

The `property_aggregator` module provides tools for managing the data in your database.

### Marking Inactive Listings

```bash
python manage_db.py  # Select option 2
```

Or programmatically:
```python
updater = IncrementalUpdater()
count = updater.mark_as_inactive(days_threshold=7)
```

## 📚 Documentation

- **README.md** - This file
- **QUICKSTART.md** - 5-minute quick start
- **INCREMENTAL_UPDATE.md** - Detailed incremental update guide
- **FILE_LIST.md** - File inventory

For detailed information about incremental updates, see [INCREMENTAL_UPDATE.md](INCREMENTAL_UPDATE.md).

## 📄 License

MIT License

## 👥 Contributing

Contributions are welcome! Please submit a pull request or open an issue to discuss your ideas.