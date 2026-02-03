"""
Export Application Database to SQLite and CSV
==============================================
Exports data from real_estate_app PostgreSQL database to:
- SQLite database (portable)
- CSV files (for easy viewing)
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import sqlite3

# --- Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'real_estate_app')
DB_USER = os.getenv('DB_USER', 'alanwang')
DB_PASS = os.getenv('DB_PASS', '')

POSTGRES_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Output paths
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'exports')
SQLITE_PATH = os.path.join(OUTPUT_DIR, 'real_estate_app.db')


def ensure_output_dir():
    """Create output directory if not exists."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: {OUTPUT_DIR}")


def export_table_to_csv(pg_engine, table_name, output_dir):
    """Export a single table to CSV."""
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, pg_engine)
    csv_path = os.path.join(output_dir, f"{table_name}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  ✓ {table_name}.csv ({len(df)} rows)")
    return df


def export_to_sqlite(dataframes, sqlite_path):
    """Export dataframes to SQLite database."""
    # Remove existing file
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)
    
    # Create SQLite connection
    conn = sqlite3.connect(sqlite_path)
    
    for table_name, df in dataframes.items():
        df.to_sql(table_name, conn, index=False, if_exists='replace')
        print(f"  ✓ {table_name} ({len(df)} rows)")
    
    conn.close()
    
    # Show file size
    size_mb = os.path.getsize(sqlite_path) / (1024 * 1024)
    print(f"\nSQLite file: {sqlite_path}")
    print(f"Size: {size_mb:.2f} MB")


def main():
    print("=" * 60)
    print("Export Database to SQLite and CSV")
    print("=" * 60)
    print(f"Source: {POSTGRES_URL}")
    print()
    
    # Ensure output directory
    ensure_output_dir()
    
    # Connect to PostgreSQL
    try:
        pg_engine = create_engine(POSTGRES_URL)
        print("Connected to PostgreSQL.\n")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return 1
    
    # Tables to export
    tables = ['agents', 'listings', 'condo_basic']
    
    # Export to CSV
    print("Exporting to CSV...")
    dataframes = {}
    for table in tables:
        try:
            df = export_table_to_csv(pg_engine, table, OUTPUT_DIR)
            dataframes[table] = df
        except Exception as e:
            print(f"  ✗ {table}: {e}")
    
    print()
    
    # Export to SQLite
    print("Exporting to SQLite...")
    export_to_sqlite(dataframes, SQLITE_PATH)
    
    print("\n" + "=" * 60)
    print("Export complete!")
    print("=" * 60)
    print(f"\nFiles created:")
    print(f"  - {SQLITE_PATH}")
    for table in tables:
        print(f"  - {os.path.join(OUTPUT_DIR, table + '.csv')}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
