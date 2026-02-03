"""
Unified Database Setup for Real Estate Application
===================================================
This script sets up the final application database (real_estate_app) with 
properly structured tables:
- agents: Agent information
- listings: Property listings with foreign key to agents
- condo_basic: Condo/HDB basic information (from new data source)

The listings table can be enriched with condo_basic data via matching.
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
    
    id = Column(Integer, primary_key=True, autoincrement=True)
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
    
    condo_id = Column(Integer, ForeignKey('condo_basic.id'), nullable=True)
    condo = relationship("CondoBasic", backref="listings")
    
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


def load_csv_data():
    """Load aggregated listings from CSV."""
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    csv_path = os.path.join(base_dir, 'aggregated_listings.csv')
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV not found at {csv_path}")
        return None
    
    print(f"Loading data from: {csv_path}")
    df = pd.read_csv(csv_path)
    df = df.replace({np.nan: None})
    print(f"Loaded {len(df)} listings")
    return df


def normalize_name(name):
    """Normalize property name for matching."""
    if not name or pd.isna(name):
        return ""
    name = str(name).lower().strip()
    for pattern in ['condominium', 'condo', 'residence', 'residences', 'apartment', 'apartments', '@', 'the', 'at']:
        name = name.replace(pattern, ' ')
    return ' '.join(name.split())


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
    
    # Create tables (DROP ALL first to ensure schema update)
    # WARNING: This deletes all data!
    Base.metadata.drop_all(engine)
    print("Dropped all existing tables.")
    Base.metadata.create_all(engine)
    print("Created database tables.")
    
    # Clear existing data (be careful in production!)
    try:
        session.query(Listing).delete()
        session.query(Agent).delete()
        # Keep condo_basic data if it exists
        session.commit()
        print("Cleared existing listings and agents.")
    except Exception as e:
        print(f"Warning: {e}")
        session.rollback()
    
    # Load condo_basic for matching
    condo_data = {}
    try:
        condos = session.query(CondoBasic).all()
        for c in condos:
            normalized = normalize_name(c.condo_name)
            if normalized:
                condo_data[normalized] = c
        print(f"Loaded {len(condo_data)} condo records for matching.")
    except Exception as e:
        print(f"No condo_basic data available: {e}")
    
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
            title = safe_str(row.get('title'))
            normalized_title = normalize_name(title)
            
            condo = None
            match_score = None
            match_method = None
            
            if normalized_title and condo_data:
                # Exact match
                if normalized_title in condo_data:
                    condo = condo_data[normalized_title]
                    match_score = 100
                    match_method = 'exact'
                else:
                    # Simple fuzzy match (check if title contains condo name or vice versa)
                    for condo_name, c in condo_data.items():
                        if condo_name in normalized_title or normalized_title in condo_name:
                            condo = c
                            match_score = 80
                            match_method = 'partial'
                            break
            
            if condo:
                matched_with_condo += 1
            
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
                built_year=safe_str(row.get('built_year')),
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
                postal_code=condo.postal_code if condo else None,
                district=condo.district if condo else None,
                neighbourhood=condo.neighbourhood if condo else None,
                latitude=condo.latitude if condo else None,
                longitude=condo.longitude if condo else None,
                street_name=condo.street_name if condo else None,
                mrt_nearby=condo.mrt_nearby if condo else None,
                developer_name=condo.developer_name if condo else None,
                total_units=condo.total_units if condo else None,
                num_floors=condo.num_floors if condo else None,
                num_blocks=condo.num_blocks if condo else None,
                has_swimming_pool=condo.has_swimming_pool if condo else False,
                has_gym=condo.has_gym if condo else False,
                has_tennis_court=condo.has_tennis_court if condo else False,
                has_security=condo.has_security if condo else False,
                has_parking=condo.has_parking if condo else False,
                amenities_json=condo.amenities_json if condo else None,
                facilities_json=condo.facilities_json if condo else None,
                
                match_score=match_score,
                match_method=match_method,
                is_active=True
            )
            session.add(listing)
            listings_created += 1

        except Exception as e:
            print(f"Error processing row {idx}: {e}")
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
    print(f"Matched with condo_basic: {matched_with_condo} ({matched_with_condo/listings_created*100:.1f}%)")
    
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
    
    # Load data
    df = load_csv_data()
    if df is None or len(df) == 0:
        print("No data to process.")
        return 1
    
    # Ingest
    ingest_all_data(df, engine)
    
    print("\nDatabase setup complete!")
    print(f"Tables: agents, listings, condo_basic")
    return 0


if __name__ == '__main__':
    sys.exit(main())
