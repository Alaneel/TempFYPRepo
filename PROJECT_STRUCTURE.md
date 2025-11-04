# Project Structure

This document outlines the structure of the PropertyGuru Scraper and Aggregator project.

## 📂 Core Modules

The project is divided into two main modules:

1.  **`property_scraper`**: A Scrapy project responsible for crawling and extracting data from PropertyGuru.
2.  **`property_aggregator`**: A collection of scripts for managing the data in the PostgreSQL database.

## 📁 File and Directory Layout

```
/
├── property_scraper/       # Scrapy project for scraping
│   ├── property_scraper/
│   │   ├── spiders/
│   │   │   └── propertyguru_spider.py  # The main spider for PropertyGuru
│   │   ├── items.py                  # Defines the data structure for scraped items
│   │   ├── middlewares.py            # Custom Scrapy middlewares
│   │   ├── pipelines.py              # Processes and stores scraped items in PostgreSQL
│   │   ├── settings.py               # Scrapy project settings
│   │   └── __init__.py
│   ├── scrapy.cfg                    # Scrapy configuration file
│   └── __init__.py
│
├── property_aggregator/    # Tools for data aggregation and management
│   ├── create_tables.py          # Initializes the database schema in PostgreSQL
│   ├── database.py               # Database connection and session management (SQLAlchemy)
│   ├── mark_inactive_listings.py # Marks listings that are no longer active
│   ├── models.py                 # SQLAlchemy ORM models for the database tables
│   └── __init__.py
│
├── data/                     # Data directory (logs, exports, etc.)
│
├── logs/                     # Directory for log files
│
├── config.py                 # Main configuration file (create from config_example.py)
├── config_example.py         # Example configuration file
├── requirements.txt          # Python dependencies for the project
├── FILE_LIST.md              # File list and quick reference
├── PROJECT_STRUCTURE.md      # This file
├── QUICKSTART.md             # Quick start guide
└── README.md                 # Main project documentation
```

## 💾 Database

The project uses a **PostgreSQL** database as the central data store. The database schema is defined in `property_aggregator/models.py` using SQLAlchemy ORM.

### Database Tables

- **listings**: Stores property listing information including:
  - Property details (address, price, bedrooms, bathrooms, etc.)
  - Listing metadata (created_date, updated_date, is_active)
  - Agent information
  - Contact details

## ⚙️ Configuration

The primary configuration file is `config.py`. This file contains settings for:

*   Database connection (`DATABASE_URL`)
*   Logging configuration (`LOG_LEVEL`, `LOG_ROTATION`, `LOG_RETENTION`)

Additional Scrapy settings are configured in `property_scraper/property_scraper/settings.py`:
*   `CLOUDBYPASS_APIKEY`: Your API key for the CloudBypass service
*   `CLOUDBYPASS_PROXY`: Your proxy configuration

## 🔄 Workflow

1. **Configuration**: Set up `config.py` with your PostgreSQL connection string
2. **Database Setup**: Run `python -m property_aggregator.create_tables` to initialize the database
3. **Scraping**: Run `scrapy crawl propertyguru` to start the scraper
4. **Maintenance**: Periodically run `python -m property_aggregator.mark_inactive_listings` to update listing status

## 📚 Documentation Files

- **README.md**: Complete project documentation and usage guide
- **QUICKSTART.md**: 5-minute quick start guide
- **FILE_LIST.md**: File inventory and quick reference
- **PROJECT_STRUCTURE.md**: This file - detailed project structure

## ✨ Current Architecture

- **Framework**: Scrapy for web scraping
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Python Version**: 3.7+
- **Date Updated**: 2025-11-04

---

**Note**: This project uses a modern PostgreSQL + Scrapy architecture. All legacy components have been removed.
