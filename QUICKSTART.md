# Quickstart Guide

This guide will help you get the PropertyGuru Scraper and Aggregator project up and running quickly.

## 🚀 5-Minute Setup

### 1️⃣ Install Dependencies

Make sure you have Python 3.x and pip installed. Then, install the required packages:

```bash
pip install -r requirements.txt
```

### 2️⃣ Configure the Project

1.  **Create the configuration file:**

    Copy the example configuration file to create your own local configuration:

    ```bash
    cp config_example.py config.py
    ```

2.  **Edit `config.py`:**

    Open `config.py` and provide the following essential information:

    *   **`DATABASE_URL`**: Your PostgreSQL database connection string.
        ```python
        # Example: "postgresql://user:password@localhost:5432/real_estate_db"
        DATABASE_URL = "your_postgresql_connection_string"
        ```

    *   **Scrapy Settings**:
        You also need to configure the Scrapy settings in `property_scraper/property_scraper/settings.py`.
        *   `CLOUDBYPASS_APIKEY`: Your API key for the CloudBypass service.
        *   `CLOUDBYPASS_PROXY`: Your proxy configuration.

### 3️⃣ Set Up the Database

Before you can start scraping, you need to create the database tables. Run the following command:

```bash
python -m property_aggregator.create_tables
```

This will create the `listings` table in the PostgreSQL database you configured in `config.py`.

### 4️⃣ Start Scraping

Now you are ready to start the scraper. Run the following command from the project root directory:

```bash
scrapy crawl propertyguru
```

The spider will start crawling PropertyGuru, and the scraped data will be saved to your PostgreSQL database.

## ✅ Next Steps

*   **Monitor the scraper**: You can view the scraper's progress in the console output.
*   **Check the data**: Connect to your PostgreSQL database to see the scraped data in the `listings` table.
*   **Maintain your data**: Periodically run the `mark_inactive_listings.py` script to keep your data up-to-date:
    ```bash
    python -m property_aggregator.mark_inactive_listings
    ```