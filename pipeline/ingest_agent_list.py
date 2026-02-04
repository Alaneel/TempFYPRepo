"""
Ingest agent_list.csv into PostgreSQL agent_list table.

This script reads the agent_list.csv file and inserts/updates records
into the agent_list table.
"""

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'real_estate_app')
DB_USER = os.getenv('DB_USER', 'alanwang')
DB_PASS = os.getenv('DB_PASS', '')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def clean_phone(val):
    """Clean phone number."""
    if val is None or pd.isna(val):
        return None
    s = str(val)
    # Remove .0 suffix from float conversion
    if s.endswith('.0'):
        s = s[:-2]
    return s if s else None


def parse_date(val):
    """Parse date string to date object."""
    if val is None or pd.isna(val):
        return None
    try:
        return pd.to_datetime(val).date()
    except:
        return None


def main():
    print("=" * 60)
    print("Agent List CSV Ingestion")
    print("=" * 60)
    
    # Determine CSV path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(base_dir, 'data', 'own', 'agent_list.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return 1
    
    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} records")
    
    # Connect to database
    try:
        engine = create_engine(DATABASE_URL)
        print(f"Connected to database: {DB_NAME}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return 1
    
    # Create table if not exists
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS agent_list (
        id BIGINT PRIMARY KEY,
        cea_number VARCHAR(8),
        agent_name VARCHAR(100),
        phone VARCHAR(20),
        company_name VARCHAR(100),
        agency_license VARCHAR(9),
        license_expiry DATE,
        registration_date DATE,
        photo_url VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_agent_list_cea ON agent_list(cea_number);
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        print("Table agent_list ensured.")
    
    # Prepare insert/update using UPSERT
    upsert_sql = """
    INSERT INTO agent_list (
        id, cea_number, agent_name, phone, company_name, 
        agency_license, license_expiry, registration_date, photo_url,
        created_at, updated_at
    ) VALUES (
        :id, :cea_number, :agent_name, :phone, :company_name,
        :agency_license, :license_expiry, :registration_date, :photo_url,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (id) DO UPDATE SET
        cea_number = EXCLUDED.cea_number,
        agent_name = EXCLUDED.agent_name,
        phone = EXCLUDED.phone,
        company_name = EXCLUDED.company_name,
        agency_license = EXCLUDED.agency_license,
        license_expiry = EXCLUDED.license_expiry,
        registration_date = EXCLUDED.registration_date,
        photo_url = EXCLUDED.photo_url,
        updated_at = CURRENT_TIMESTAMP;
    """
    
    inserted = 0
    errors = 0
    
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            try:
                params = {
                    'id': int(row['id']),
                    'cea_number': row.get('cea_number') if pd.notna(row.get('cea_number')) else None,
                    'agent_name': row.get('agent_name') if pd.notna(row.get('agent_name')) else None,
                    'phone': clean_phone(row.get('phone')),
                    'company_name': row.get('company_name') if pd.notna(row.get('company_name')) else None,
                    'agency_license': row.get('agency_license') if pd.notna(row.get('agency_license')) else None,
                    'license_expiry': parse_date(row.get('license_expiry')),
                    'registration_date': parse_date(row.get('registration_date')),
                    'photo_url': row.get('photo_url') if pd.notna(row.get('photo_url')) else None,
                }
                conn.execute(text(upsert_sql), params)
                inserted += 1
            except Exception as e:
                print(f"Error on row {idx}: {e}")
                errors += 1
        
        conn.commit()
    
    print()
    print("=" * 60)
    print(f"Ingestion Complete!")
    print(f"=" * 60)
    print(f"Records inserted/updated: {inserted}")
    print(f"Errors: {errors}")
    
    # Verify
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM agent_list"))
        count = result.scalar()
        print(f"Total records in agent_list table: {count}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
