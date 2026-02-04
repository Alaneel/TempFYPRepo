import os
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import datetime
import numpy as np

# --- Configuration ---
# SQLite file path relative to this script
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
DB_PATH = os.path.join(BASE_DIR, 'aggregated_data.db')
DATABASE_URL = f"sqlite:///{DB_PATH}"

Base = declarative_base()

# --- Models ---
# Same models as ingest_data.py but compatible with SQLite (e.g. no fancy Postgres types if used)

class Agent(Base):
    __tablename__ = 'agents'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    mobile = Column(String(50), nullable=True)
    cea = Column(String(50), nullable=True)
    description = Column(Text, nullable=True)
    url = Column(String(500), nullable=True)
    source_id = Column(String(100), nullable=True)
    
    __table_args__ = (
        UniqueConstraint('name', 'mobile', name='uix_agent_name_mobile'),
    )

class Listing(Base):
    __tablename__ = 'listings'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Core Fields
    title = Column(String(500))
    address = Column(String(500))
    
    # Prices
    display_price = Column(String(100))
    price = Column(Float)
    display_psf = Column(String(100))
    psf = Column(Float)
    
    # Details
    beds = Column(Integer)
    baths = Column(Integer)
    sqft = Column(Integer)
    built_year = Column(String(50))
    property_type = Column(String(100))
    tenure = Column(String(100))
    
    # Text / Metadata
    nearby_text = Column(Text)
    description = Column(Text) 
    url = Column(String(500), unique=True)
    source = Column(String(50))
    
    posted_date = Column(String(100))
    buy_rent = Column(String(20))
    
    # Foreign Key
    agent_id = Column(Integer, ForeignKey('agents.id'))
    agent = relationship("Agent", backref="listings")
    
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

def ingest_data():
    print(f"Connecting to database: {DATABASE_URL}")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    try:
        engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Clean existing data to ensure no stale records remain
    try:
        session.query(Listing).delete()
        # session.query(Agent).delete() # Optional: keep agents for now
        session.commit()
        print("Cleared existing listings from database.")
    except Exception as e:
        print(f"Error clearing database: {e}")
        session.rollback()

    csv_path = os.path.join(BASE_DIR, 'aggregated_listings.csv')
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        return
        
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    # Robust NaN handling
    df = df.replace({np.nan: None})
    
    agents_processed = 0
    listings_processed = 0
    
    for index, row in df.iterrows():
        # 1. Upsert Agent
        agent_name = row.get('agent_name')
        if not agent_name:
            agent_name = 'Unknown Agent'
            
        agent_mobile = row.get('mobile')
        if agent_mobile is not None:
             agent_mobile = str(agent_mobile).replace('.0', '')
        
        agent_cea = row.get('cea')
        agent_desc = row.get('agent_description')
        agent_url = row.get('agent_url')
        agent_source_id = row.get('agent_id')
        
        # Check if agent exists
        criteria = {'name': agent_name}
        if agent_mobile:
            criteria['mobile'] = agent_mobile
        
        agent = session.query(Agent).filter_by(**criteria).first()
            
        if not agent:
            agent = Agent(
                name=agent_name,
                mobile=agent_mobile,
                cea=agent_cea,
                description=agent_desc,
                url=agent_url,
                source_id=str(agent_source_id) if agent_source_id else None
            )
            session.add(agent)
            session.flush()
            agents_processed += 1
            
        def get_int(val):
            if val is None: return None
            import re
            try:
               s = str(val)
               digits = re.findall(r'(\d+)', s)
               return int(digits[0]) if digits else None
            except:
               return None

        # 2. Insert Listing
        listing_url = row.get('url')
        existing_listing = session.query(Listing).filter_by(url=listing_url).first()
        
        if existing_listing:
            existing_listing.title = row.get('title')
            existing_listing.address = row.get('address')
            existing_listing.display_price = row.get('display_price')
            existing_listing.price = row.get('price')
            existing_listing.display_psf = row.get('display_psf')
            existing_listing.psf = row.get('psf')
            existing_listing.beds = get_int(row.get('beds'))
            existing_listing.baths = get_int(row.get('baths'))
            existing_listing.sqft = get_int(row.get('sqft'))
            existing_listing.built_year = row.get('built_year')
            existing_listing.property_type = row.get('property_type')
            existing_listing.tenure = row.get('tenure')
            existing_listing.nearby_text = row.get('nearby_text')
            existing_listing.description = row.get('description')
            existing_listing.source = row.get('source')
            existing_listing.posted_date = row.get('posted_date')
            existing_listing.buy_rent = row.get('buy_rent')
            existing_listing.updated_at = datetime.datetime.utcnow()
        else:
            listing = Listing(
                title=row.get('title'),
                address=row.get('address'),
                display_price=row.get('display_price'),
                price=row.get('price'),
                display_psf=row.get('display_psf'),
                psf=row.get('psf'),
                beds=get_int(row.get('beds')),
                baths=get_int(row.get('baths')),
                sqft=get_int(row.get('sqft')),
                built_year=row.get('built_year'),
                property_type=row.get('property_type'),
                tenure=row.get('tenure'),
                nearby_text=row.get('nearby_text'),
                description=row.get('description'),
                url=listing_url,
                source=row.get('source'),
                posted_date=row.get('posted_date'),
                buy_rent=row.get('buy_rent'),
                agent_id=agent.id
            )
            session.add(listing)
            listings_processed += 1
            
        if index % 1000 == 0:
            session.commit()
            print(f"Processed {index + 1} rows...")

    session.commit()
    print(f"Ingestion Complete. DB Saved to: {os.path.abspath(DB_PATH)}")
    print(f"New Agents: {agents_processed}, Listings: {listings_processed}")

if __name__ == '__main__':
    ingest_data()
