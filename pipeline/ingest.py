"""
Unified Database Setup for Real Estate Application
===================================================
This script sets up the final application database (real_estate_app) with
properly structured tables:
- agents: Agent information
- listings: Property listings with foreign key to agents
- condo_basic: Condo/HDB basic information (sourced directly from data/basic/property_basic.csv)

The listings table is enriched with condo_basic data by matching:
  listing.title  ==  condo_basic.condo_name  (case-insensitive exact match, with substring fallback)

Note: condo_basic is ingested directly from CSV before listings, similar to agent_list.
"""

import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import datetime
import re

# --- Configuration ---
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'real_estate_app')
DB_USER = os.getenv('DB_USER', 'alanwang')
DB_PASS = os.getenv('DB_PASS', '')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

Base = declarative_base()


# =====================================================
# Database Models
# =====================================================

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String, nullable=True)
    role = Column(String, default="customer") # customer, agent, admin
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    
    # Relationships
    agent_profile = relationship("Agent", back_populates="user", uselist=False)

class Agent(Base):
    """Agent information table."""
    __tablename__ = 'agents'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    mobile = Column(String(50), nullable=True)
    cea = Column(String(50), nullable=True)
    rating = Column(Float, nullable=True)
    description = Column(Text, nullable=True)
    url = Column(String(500), nullable=True)
    source_id = Column(String(100), nullable=True)
    
    # Link to User account (optional, for claimed profiles)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True, unique=True)
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    
    user = relationship("User", back_populates="agent_profile")

    __table_args__ = (
        UniqueConstraint('name', 'mobile', name='uix_agent_name_mobile'),
    )


class CondoBasic(Base):
    """Condo/HDB basic information from new data source."""
    __tablename__ = 'condo_basic'
    
    id = Column('condo_id', Integer, primary_key=True, autoincrement=True)
    condo_name = Column(String(500), index=True)
    developer_name = Column(String(255))
    street_name = Column(String(255))
    postal_code = Column(String(20), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    tenure = Column(String(100))
    total_units = Column(Integer)
    district = Column(Integer, index=True)
    mrt_nearby = Column(String(255))
    has_swimming_pool = Column(Boolean, default=False)
    has_gym = Column(Boolean, default=False)
    has_tennis_court = Column(Boolean, default=False)
    has_security = Column(Boolean, default=False)
    has_parking = Column(Boolean, default=False)
    property_type = Column(String(100))
    top_date = Column(String(100))
    neighbourhood = Column(String(100), index=True)
    num_floors = Column(Integer)
    num_blocks = Column(Integer)
    amenities_json = Column(Text)
    facilities_json = Column(Text)
    description = Column(Text)
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


class HdbBasic(Base):
    """HDB block Directory Information."""
    __tablename__ = 'hdb_basic'
    
    hdb_id = Column(Integer, primary_key=True, autoincrement=True)
    block_number = Column(String(10), nullable=False)
    street_name = Column(Text, nullable=False)
    town = Column(Text, nullable=False)
    postal_code = Column(String(6))
    latitude = Column(Float)
    longitude = Column(Float)
    total_floors = Column(Integer)
    year_completed = Column(Integer)
    has_residential = Column(Boolean)
    has_commercial = Column(Boolean)
    has_market_hawker = Column(Boolean)
    has_multistorey_carpark = Column(Boolean)
    has_void_deck = Column(Boolean)
    total_dwelling_units = Column(Integer)
    one_room_qty = Column(Integer)
    two_room_qty = Column(Integer)
    three_room_qty = Column(Integer)
    four_room_qty = Column(Integer)
    five_room_qty = Column(Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class Listing(Base):
    """Property listing table with references to agent and condo_basic."""
    __tablename__ = 'listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core Fields
    title = Column(String(500), index=True)
    address = Column(String(500), index=True)
    
    # Prices
    display_price = Column(String(100))
    price = Column(Float, index=True)
    display_psf = Column(String(100))
    psf = Column(Float, index=True)
    
    # Details
    beds = Column(Integer, index=True)
    baths = Column(Integer)
    sqft = Column(Integer, index=True)
    built_year = Column(String(50))
    property_type = Column(String(100), index=True)
    tenure = Column(String(100), index=True)
    
    # Location (from listing or enriched from condo_basic)
    postal_code = Column(String(20), index=True)
    district = Column(Integer, index=True)
    neighbourhood = Column(String(100), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    street_name = Column(String(255))
    mrt_nearby = Column(String(255))
    nearby_text = Column(Text)
    
    # Building Info (enriched from condo_basic)
    developer_name = Column(String(255))
    total_units = Column(Integer)
    num_floors = Column(Integer)
    num_blocks = Column(Integer)
    
    # Facilities (enriched from condo_basic)
    has_swimming_pool = Column(Boolean, default=False)
    has_gym = Column(Boolean, default=False)
    has_tennis_court = Column(Boolean, default=False)
    has_security = Column(Boolean, default=False)
    has_parking = Column(Boolean, default=False)
    amenities_json = Column(Text)
    facilities_json = Column(Text)
    
    # Text / Metadata
    description = Column(Text)
    url = Column(String(500), unique=True)
    source = Column(String(50), index=True)
    posted_date = Column(String(100))
    buy_rent = Column(String(20), index=True)
    
    # Foreign Keys
    agent_id = Column(Integer, ForeignKey('agents.id'))
    agent = relationship("Agent", backref="listings")
    
    condo_id = Column(Integer, ForeignKey('condo_basic.condo_id'), nullable=True)
    condo = relationship("CondoBasic", backref="listings")
    
    hdb_id = Column(Integer, ForeignKey('hdb_basic.hdb_id'), nullable=True)
    hdb = relationship("HdbBasic", backref="listings")
    
    # Match metadata (for tracking enrichment)
    match_score = Column(Float)
    match_method = Column(String(50))
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


# =====================================================
# Helper Functions
# =====================================================

def to_python_type(val):
    """Convert numpy types to native Python types."""
    if val is None:
        return None
    if isinstance(val, (np.integer, np.int64, np.int32)):
        return int(val)
    if isinstance(val, (np.floating, np.float64, np.float32)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            return None
    if isinstance(val, np.bool_):
        return bool(val)
    if pd.isna(val):
        return None
    return val


def safe_str(val):
    """Safely convert value to string."""
    if val is None or pd.isna(val):
        return None
    return str(val)


def get_int(val):
    """Extract integer from string like '3 Beds'."""
    if val is None:
        return None
    try:
        s = str(val)
        digits = re.findall(r'(\d+)', s)
        return int(digits[0]) if digits else None
    except:
        return None


def load_sqlite_data():
    """Load aggregated listings from SQLite database."""
    import sqlite3
    
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    db_path = os.path.join(base_dir, 'aggregated.db')
    
    if not os.path.exists(db_path):
        print(f"Error: SQLite database not found at {db_path}")
        print("Run 'python pipeline/aggregate.py' first to generate the database.")
        return None
    
    print(f"Loading data from: {db_path}")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM listings", conn)
    conn.close()
    
    df = df.replace({np.nan: None})
    print(f"Loaded {len(df)} listings")
    return df


def strip_title(name):
    """Lowercase + strip for case-insensitive exact matching."""
    if not name or (isinstance(name, float) and pd.isna(name)):
        return ""
    return str(name).lower().strip()


def ingest_condo_basic(engine):
    """
    Reads condo/HDB basic data from data/basic/property_basic.csv
    and upserts into the condo_basic table.
    """
    # Ensure table exists first
    CondoBasic.__table__.create(engine, checkfirst=True)
    
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    csv_path = os.path.join(base_dir, 'basic', 'property_basic.csv')

    if not os.path.exists(csv_path):
        print(f"Warning: property_basic.csv not found at {csv_path}. Skipping condo_basic ingestion.")
        return 0

    print(f"\nLoading condo_basic from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Found {len(df)} rows in property_basic.csv")

    def _bool(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return False
        if isinstance(val, bool):
            return val
        return str(val).strip().lower() in ('true', '1', 'yes')

    def _int_or_none(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    def _float_or_none(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _str_or_none(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        return s if s else None

    upsert_sql = """
    INSERT INTO condo_basic (
        id, condo_name, developer_name, street_name, postal_code,
        latitude, longitude, tenure, total_units, district, mrt_nearby,
        has_swimming_pool, has_gym, has_tennis_court, has_security, has_parking,
        property_type, top_date, neighbourhood, num_floors, num_blocks,
        amenities_json, facilities_json, description,
        created_at, updated_at
    ) VALUES (
        :id, :condo_name, :developer_name, :street_name, :postal_code,
        :latitude, :longitude, :tenure, :total_units, :district, :mrt_nearby,
        :has_swimming_pool, :has_gym, :has_tennis_court, :has_security, :has_parking,
        :property_type, :top_date, :neighbourhood, :num_floors, :num_blocks,
        :amenities_json, :facilities_json, :description,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (id) DO UPDATE SET
        condo_name        = EXCLUDED.condo_name,
        developer_name    = EXCLUDED.developer_name,
        street_name       = EXCLUDED.street_name,
        postal_code       = EXCLUDED.postal_code,
        latitude          = EXCLUDED.latitude,
        longitude         = EXCLUDED.longitude,
        tenure            = EXCLUDED.tenure,
        total_units       = EXCLUDED.total_units,
        district          = EXCLUDED.district,
        mrt_nearby        = EXCLUDED.mrt_nearby,
        has_swimming_pool = EXCLUDED.has_swimming_pool,
        has_gym           = EXCLUDED.has_gym,
        has_tennis_court  = EXCLUDED.has_tennis_court,
        has_security      = EXCLUDED.has_security,
        has_parking       = EXCLUDED.has_parking,
        property_type     = EXCLUDED.property_type,
        top_date          = EXCLUDED.top_date,
        neighbourhood     = EXCLUDED.neighbourhood,
        num_floors        = EXCLUDED.num_floors,
        num_blocks        = EXCLUDED.num_blocks,
        amenities_json    = EXCLUDED.amenities_json,
        facilities_json   = EXCLUDED.facilities_json,
        description       = EXCLUDED.description,
        updated_at        = CURRENT_TIMESTAMP;
    """

    inserted = 0
    errors = 0
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        for idx, row in df.iterrows():
            try:
                params = {
                    'id':               int(row['id']),
                    'condo_name':       _str_or_none(row.get('condo_name')),
                    'developer_name':   _str_or_none(row.get('developer_name')),
                    'street_name':      _str_or_none(row.get('street_name')),
                    'postal_code':      _str_or_none(row.get('postal_code')),
                    'latitude':         _float_or_none(row.get('latitude')),
                    'longitude':        _float_or_none(row.get('longitude')),
                    'tenure':           _str_or_none(row.get('tenure')),
                    'total_units':      _int_or_none(row.get('total_units')),
                    'district':         _int_or_none(row.get('district')),
                    'mrt_nearby':       _str_or_none(row.get('mrt_nearby')),
                    'has_swimming_pool': _bool(row.get('has_swimming_pool')),
                    'has_gym':          _bool(row.get('has_gym')),
                    'has_tennis_court': _bool(row.get('has_tennis_court')),
                    'has_security':     _bool(row.get('has_security')),
                    'has_parking':      _bool(row.get('has_parking')),
                    'property_type':    _str_or_none(row.get('property_type')),
                    'top_date':         _str_or_none(row.get('top_date')),
                    'neighbourhood':    _str_or_none(row.get('neighbourhood')),
                    'num_floors':       _int_or_none(row.get('num_floors')),
                    'num_blocks':       _int_or_none(row.get('num_blocks')),
                    'amenities_json':   _str_or_none(row.get('amenities_json')),
                    'facilities_json':  _str_or_none(row.get('facilities_json')),
                    'description':      _str_or_none(row.get('description')),
                }
                conn.execute(sa_text(upsert_sql), params)
                inserted += 1
            except Exception as e:
                print(f"  Error on condo row {idx}: {e}")
                errors += 1
        conn.commit()

    print(f"condo_basic ingestion: {inserted} upserted, {errors} errors.")
    return inserted


# =====================================================
# Main Ingestion Logic
# =====================================================

def clean_cea(val):
    if not val or pd.isna(val):
        return None
    s = str(val).strip()
    # Handle "CEA: R029832A / L3008022J" -> "R029832A"
    s = s.replace('CEA:', '').strip()
    return s.split('/')[0].strip()

def clean_mobile(val):
    if not val or pd.isna(val):
        return None
    s = str(val)
    if s.endswith('.0'):
        s = s[:-2]
    # Handle 65 prefix
    if s.startswith('65') and len(s) == 10:
        s = s[2:]
    return s

def ingest_all_data(df, engine):
    """Ingest all data into the application database."""
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Drop and recreate only agents/listings/users tables (NOT condo_basic —
    # that was already populated from property_basic.csv before this call).
    # Use CASCADE to handle FK constraints automatically.
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        conn.execute(sa_text(
            "DROP TABLE IF EXISTS listings, agents, users CASCADE"
        ))
        conn.commit()
    for tbl in [User.__table__, Agent.__table__, Listing.__table__]:
        tbl.create(engine)
    # Ensure condo_basic and hdb_basic tables exist
    CondoBasic.__table__.create(engine, checkfirst=True)
    HdbBasic.__table__.create(engine, checkfirst=True)
    print("Recreated agents/listings tables; condo_basic & hdb_basic preserved.")
    
    # Load condo_basic for matching (keyed by lowercase condo_name)
    condo_data = {}
    try:
        condos = session.query(CondoBasic).all()
        for c in condos:
            key = strip_title(c.condo_name)
            if key:
                condo_data[key] = c
        print(f"Loaded {len(condo_data)} condo records for matching.")
    except Exception as e:
        print(f"No condo_basic data available: {e}")
        
    hdb_data = {}
    try:
        hdbs = session.query(HdbBasic).all()
        for h in hdbs:
            # Create a match key like "123 ang mo kio"
            key = strip_title(f"{h.block_number} {h.street_name}")
            if key:
                hdb_data[key] = h
        print(f"Loaded {len(hdb_data)} HDB records for matching.")
    except Exception as e:
        print(f"No hdb_basic data available: {e}")
    
    # Process data
    agents_cache = {}  # Cache to avoid duplicate agent lookups
    total = len(df)
    agents_created = 0
    listings_created = 0
    matched_with_condo = 0
    


# ... Inside ingest_all_data ...
    print(f"Starting ingestion loop with {total} rows...")
    for idx, row in df.iterrows():
        try:
            # 1. Get or create Agent
            agent_name = safe_str(row.get('agent_name')) or 'Unknown Agent'
            
            # Clean data
            raw_mobile = row.get('mobile')
            agent_mobile = clean_mobile(raw_mobile)
            
            raw_cea = row.get('cea')
            agent_cea = clean_cea(raw_cea)
            
            agent_url = safe_str(row.get('agent_url'))
            agent_rating = to_python_type(row.get('rating'))
            agent_desc = safe_str(row.get('description')) 
            # Note: description is also on listing, but CSV seems to have agent specific desc sometimes?
            # Actually CSV header shows 'description' which is likely listing description.
            # But earlier check showed 'agent_name' etc. Let's check CSV headers again.
            # Header: id,title,...,agent_name,description,agent_url,cea,mobile,rating,...
            # 'description' is likely listing description. Is there agent_description? No.
            # We will use 'rating'.
            
            # Try to match by Name + Mobile (if mobile exists) or just Name
            cache_key = agent_name # Simplify cache key to name for lookup 
            # (Note: real world agents might share names, but for this dataset name seems unique enough or primary key)
            # Better: Use Name + Mobile if available
            
            # Strategy: Find existing agent. If found, enrich data. If not, create.
            agent = None
            
            # Check cache first
            if agent_name in agents_cache:
                agent = agents_cache[agent_name]
            else:
                 # Check DB
                 agent = session.query(Agent).filter_by(name=agent_name).first()
            
            if agent:
                 # Update if missing info
                 if not agent.mobile and agent_mobile:
                     agent.mobile = agent_mobile
                 if not agent.cea and agent_cea:
                     agent.cea = agent_cea
                 if not agent.url and agent_url:
                     agent.url = agent_url
                 if agent_rating is not None:
                     agent.rating = agent_rating
                 
                 session.add(agent) # Mark for update
            else:
                agent = Agent(
                    name=agent_name,
                    mobile=agent_mobile,
                    cea=agent_cea,
                    url=agent_url,
                    rating=agent_rating,
                    source_id=safe_str(row.get('agent_id'))
                )
                session.add(agent)
                session.flush() # Get ID
                agents_created += 1
                
            agents_cache[agent_name] = agent
        
            # 2. Match with condo_basic
            # Strategy: exact case-insensitive match of listing title == condo_name,
            # with substring fallback (title contains condo_name or vice versa).
            title = safe_str(row.get('title'))
            title_key = strip_title(title)

            condo = None
            match_score = None
            match_method = None

            if title_key and condo_data:
                # Exact match (case-insensitive)
                if title_key in condo_data:
                    condo = condo_data[title_key]
                    match_score = 100
                    match_method = 'exact'
                else:
                    # Word-boundary fallback: condo_name must appear as whole
                    # words inside the title (or vice versa).
                    # Use \b so "1 canberra" does NOT match "101 canberra".
                    for condo_key, c in condo_data.items():
                        if not condo_key:
                            continue
                        pattern = r'\b' + re.escape(condo_key) + r'\b'
                        if re.search(pattern, title_key) or re.search(
                            r'\b' + re.escape(title_key) + r'\b', condo_key
                        ):
                            condo = c
                            match_score = 80
                            match_method = 'partial'
                            break
            
            if condo:
                matched_with_condo += 1
                
            hdb = None
            if not condo and hdb_data:
                # 2.5 Match with hdb_basic
                # HDB addresses usually look like "123 Ang Mo Kio Ave 4"
                title_address_key = strip_title(f"{safe_str(row.get('title'))} {safe_str(row.get('address'))}")
                
                # Check if property is marked as HDB
                is_hdb = 'hdb' in str(row.get('property_type', '')).lower() or 'hdb' in title_address_key
                
                if is_hdb:
                    # Look for block + street patterns or just direct matches
                    for hdb_key, h in hdb_data.items():
                        if not hdb_key:
                            continue
                        
                        pattern = r'\b' + re.escape(hdb_key) + r'\b'
                        if re.search(pattern, title_address_key):
                            hdb = h
                            match_score = 90
                            match_method = 'hdb_block_street'
                            break
            
            if hdb:
                # Add to a counter if needed
                pass
            
            # 3. Create Listing
            listing = Listing(
                title=title,
                address=safe_str(row.get('address')),
                display_price=safe_str(row.get('display_price')),
                price=to_python_type(row.get('price')),
                display_psf=safe_str(row.get('display_psf')),
                psf=to_python_type(row.get('psf')),
                beds=get_int(row.get('beds')),
                baths=get_int(row.get('baths')),
                sqft=get_int(row.get('sqft')),
                property_type=safe_str(row.get('property_type')),
                tenure=safe_str(row.get('tenure')),
                nearby_text=safe_str(row.get('nearby_text')),
                description=safe_str(row.get('description')),
                url=safe_str(row.get('url')),
                source=safe_str(row.get('source')),
                posted_date=safe_str(row.get('posted_date')),
                buy_rent=safe_str(row.get('buy_rent')),
                agent_id=agent.id,
                
                # Enriched from condo_basic if matched
                condo_id=condo.id if condo else None,
                
                # Enriched from condo OR hdb
                postal_code=condo.postal_code if condo else (hdb.postal_code if hdb else None),
                district=condo.district if condo else None,
                neighbourhood=condo.neighbourhood if condo else (hdb.town if hdb else None),
                latitude=condo.latitude if condo else (hdb.latitude if hdb else None),
                longitude=condo.longitude if condo else (hdb.longitude if hdb else None),
                street_name=condo.street_name if condo else (f"Blk {hdb.block_number} {hdb.street_name}" if hdb else None),
                mrt_nearby=condo.mrt_nearby if condo else None,
                developer_name=condo.developer_name if condo else ('HDB' if hdb else None),
                total_units=condo.total_units if condo else (hdb.total_dwelling_units if hdb else None),
                num_floors=condo.num_floors if condo else (hdb.total_floors if hdb else None),
                num_blocks=condo.num_blocks if condo else None,
                has_swimming_pool=condo.has_swimming_pool if condo else False,
                has_gym=condo.has_gym if condo else False,
                has_tennis_court=condo.has_tennis_court if condo else False,
                has_security=condo.has_security if condo else False,
                has_parking=condo.has_parking if condo else (hdb.has_multistorey_carpark if hdb else False),
                amenities_json=condo.amenities_json if condo else None,
                facilities_json=condo.facilities_json if condo else None,
                
                hdb_id=hdb.hdb_id if hdb else None,
                built_year=safe_str(row.get('built_year')) or (str(hdb.year_completed) if hdb and hdb.year_completed else None),
                
                match_score=match_score,
                match_method=match_method,
                is_active=True
            )
            session.add(listing)
            listings_created += 1

        except Exception as e:
            print(f"Error processing row {idx}: {e}")
            session.rollback()
            continue
        
        if (idx + 1) % 1000 == 0:
            session.commit()
            print(f"Processed {idx + 1}/{total}...")
    
    session.commit()
    
    print(f"\n{'='*50}")
    print(f"Ingestion Complete!")
    print(f"{'='*50}")
    print(f"Agents created: {agents_created}")
    print(f"Listings created: {listings_created}")
    percent_matched = (matched_with_condo/listings_created*100) if listings_created > 0 else 0
    print(f"Matched with condo_basic: {matched_with_condo} ({percent_matched:.1f}%)")
    
    return listings_created


def main():
    print("=" * 60)
    print("Unified Database Setup - Real Estate Application")
    print("=" * 60)
    print(f"Database: {DATABASE_URL}")
    print()

    # Connect
    try:
        engine = create_engine(DATABASE_URL)
        print("Connected to database.")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return 1

    # Step 1 — Ingest condo_basic directly from property_basic.csv
    # (must happen BEFORE ingest_all_data so listings can be matched)
    ingest_condo_basic(engine)

    # Step 2 — Load aggregated listings from SQLite
    df = load_sqlite_data()
    if df is None or len(df) == 0:
        print("No data to process.")
        return 1

    # Step 3 — Ingest agents + listings (enriched via condo_basic matching)
    ingest_all_data(df, engine)

    print("\nDatabase setup complete!")
    print(f"Tables: agents, listings, condo_basic")
    return 0


if __name__ == '__main__':
    sys.exit(main())
