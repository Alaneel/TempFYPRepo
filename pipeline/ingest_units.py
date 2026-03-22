"""
Ingest Unit-Level Skeleton Data
==============================
Generates "skeleton" unit records for all properties in hdb_basic and condo_basic.
Enriches these records by extracting exact unit numbers from the listing history.
"""

import os
import sys
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# --- Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'real_estate_fyp')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def generate_hdb_units(engine):
    """
    Generate skeleton unit records for HDB blocks.
    Pattern: Floor-Unit (e.g., #10-123)
    """
    print("Generating HDB unit skeletons...")
    with engine.connect() as conn:
        # Fetch HDB basic info
        hdbs = conn.execute(text("SELECT hdb_id, block_number, street_name, total_floors, total_dwelling_units FROM hdb_basic")).fetchall()
        
        inserted = 0
        for h in hdbs:
            hdb_id, blk, street, floors, total_units = h
            if not floors or floors <= 0: floors = 10 # fallback
            if not total_units or total_units <= 0: total_units = 100 # fallback
            
            units_per_floor = max(1, round(total_units / floors))
            
            # Generate unit numbers like #01-01, #01-02 ...
            for f in range(1, floors + 1):
                floor_str = str(f).zfill(2)
                for u in range(1, units_per_floor + 1):
                    unit_str = str(u).zfill(2)
                    unit_num = f"{floor_str}-{unit_str}"
                    
                    # Insert stub (UPSERT style to avoid dups)
                    conn.execute(text("""
                        INSERT INTO hdb_unit (hdb_id, unit_number, floor_level, listing_status)
                        VALUES (:hdb_id, :unit_num, :floor, 'available')
                        ON CONFLICT DO NOTHING
                    """), {"hdb_id": hdb_id, "unit_num": unit_num, "floor": f})
                    inserted += 1
            
            if inserted % 1000 == 0:
                conn.commit()
                print(f"  Processed {inserted} HDB units...")
        
        conn.commit()
    print(f"HDB skeleton complete: {inserted} units created.")

def generate_condo_units(engine):
    """
    Generate skeleton unit records for Condos.
    Pattern: Unit 1 to total_units
    """
    print("Generating Condo unit skeletons...")
    with engine.connect() as conn:
        condos = conn.execute(text("SELECT condo_id, condo_name, total_units, num_floors FROM condo_basic")).fetchall()
        
        inserted = 0
        for c in condos:
            condo_id, name, total_units, floors = c
            if not total_units or total_units <= 0: total_units = 50 # fallback
            
            # For condos, we often don't know the exact #floor-unit mapping without floorplans.
            # We'll create generic stubs for now: Unit 1, Unit 2...
            # If we find actual unit numbers in listings later, we'll update these.
            for u in range(1, total_units + 1):
                unit_num = str(u)
                conn.execute(text("""
                    INSERT INTO condo_unit (condo_id, unit_number, listing_status)
                    VALUES (:condo_id, :unit_num, 'available')
                    ON CONFLICT DO NOTHING
                """), {"condo_id": condo_id, "unit_num": unit_num})
                inserted += 1
            
            if inserted % 1000 == 0:
                conn.commit()
                print(f"  Processed {inserted} Condo units...")
        
        conn.commit()
    print(f"Condo skeleton complete: {inserted} units created.")

def enrich_from_listings(engine):
    """
    Scrape 'listings' table for exact unit numbers in addresses or descriptions.
    Regex for # floor-unit: #(\d{2})[-/](\d{2,4})
    """
    print("Enriching unit tables from existing listing data...")
    with engine.connect() as conn:
        # Match HDB Listings first
        hdb_listings = conn.execute(text("""
            SELECT l.id, l.hdb_id, l.address, l.title, l.beds, l.sqft, l.price, l.psf
            FROM listings l
            WHERE l.hdb_id IS NOT NULL AND (l.address ~ '#\d{2}[-/]\d{2,4}' OR l.title ~ '#\d{2}[-/]\d{2,4}')
        """)).fetchall()
        
        hdb_updates = 0
        for l in hdb_listings:
            lid, hid, addr, title, beds, sqft, price, psf = l
            # Extract unit number
            match = re.search(r'#(\d{2})[-/](\d{2,4})', f"{addr} {title}")
            if match:
                unit_num = f"{match.group(1)}-{match.group(2)}"
                floor = int(match.group(1))
                
                # Update HDB Unit record
                conn.execute(text("""
                    UPDATE hdb_unit 
                    SET size_sqm = :size, price = :price, price_per_sqm = :ppsqm
                    WHERE hdb_id = :hid AND unit_number = :unit_num
                """), {
                    "size": (sqft * 0.092903) if sqft else None,
                    "price": price,
                    "ppsqm": (psf / 0.092903) if psf else None,
                    "hid": hid,
                    "unit_num": unit_num
                })
                hdb_updates += 1
        
        # Match Condo Listings
        condo_listings = conn.execute(text("""
            SELECT l.id, l.condo_id, l.address, l.title, l.beds, l.baths, l.sqft, l.price, l.psf
            FROM listings l
            WHERE l.condo_id IS NOT NULL AND (l.address ~ '#\d{2}[-/]\d{2,4}' OR l.title ~ '#\d{2}[-/]\d{2,4}')
        """)).fetchall()
        
        condo_updates = 0
        for l in condo_listings:
            lid, cid, addr, title, beds, baths, sqft, price, psf = l
            match = re.search(r'#(\d{2})[-/](\d{2,4})', f"{addr} {title}")
            if match:
                unit_num = f"{match.group(1)}-{match.group(2)}"
                floor = int(match.group(1))
                
                # Try to find existing skeleton or create new if numbering is complex
                conn.execute(text("""
                    INSERT INTO condo_unit (condo_id, unit_number, floor_level, bedrooms, bathrooms, size_sqm, price, price_per_sqm)
                    VALUES (:cid, :unit_num, :floor, :beds, :baths, :size, :price, :ppsqm)
                    ON CONFLICT (unit_id) DO UPDATE SET
                        floor_level = EXCLUDED.floor_level,
                        bedrooms = EXCLUDED.bedrooms,
                        bathrooms = EXCLUDED.bathrooms,
                        size_sqm = EXCLUDED.size_sqm,
                        price = EXCLUDED.price
                """), {
                    "cid": cid, "unit_num": unit_num, "floor": floor, "beds": beds, "baths": baths,
                    "size": (sqft * 0.092903) if sqft else None,
                    "price": price, "ppsqm": (psf / 0.092903) if psf else None
                })
                condo_updates += 1
                
        conn.commit()
    print(f"Enrichment complete: {hdb_updates} HDB units and {condo_updates} Condo units updated from listings.")

def main():
    engine = create_engine(DATABASE_URL)
    
    # Run skeleton generation
    generate_hdb_units(engine)
    generate_condo_units(engine)
    
    # Run enrichment
    enrich_from_listings(engine)
    
    print("\nUnit Master Directory Ingestion Complete!")

if __name__ == "__main__":
    main()
