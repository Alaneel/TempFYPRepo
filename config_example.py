"""
PropertyGuru Scraper Configuration Example

Instructions:
1. Copy this file to config.py
2. Fill in your actual configuration details.
3. The application will use the values from config.py.
"""


class Config:
    """Configuration class for the PropertyGuru Scraper."""

    # ==================== Database Configuration (Required) ====================
    # PostgreSQL connection string
    # Format: "postgresql://<username>:<password>@<host>:<port>/<database_name>"
    # Example: "postgresql://user:password@localhost:5432/real_estate_db"
    DATABASE_URL = "postgresql://user:password@localhost:5432/real_estate_db"

    # ==================== Scrapy Settings ====================
    # Note: The following settings are configured in:
    # property_scraper/property_scraper/settings.py
    # Please configure them there, not in this file:
    # - CLOUDBYPASS_APIKEY: Your API key for the CloudBypass service
    # - CLOUDBYPASS_PROXY: Your proxy configuration

    # ==================== Logging Configuration ====================
    LOG_LEVEL = 'INFO'  # Log level: DEBUG, INFO, WARNING, ERROR
    LOG_ROTATION = '500 MB'  # Log file size limit
    LOG_RETENTION = '30 days'  # Log retention period
