from sqlalchemy import Column, Integer, String, Float, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class Listing(Base):
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
    
    # Location
    postal_code = Column(String(20), index=True)
    district = Column(Integer, index=True)
    neighbourhood = Column(String(100), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    street_name = Column(String(255))
    mrt_nearby = Column(String(255))
    nearby_text = Column(Text)
    
    # Building Info
    developer_name = Column(String(255))
    total_units = Column(Integer)
    num_floors = Column(Integer)
    num_blocks = Column(Integer)
    
    # Facilities
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
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Foreign Keys
    agent_id = Column(Integer, ForeignKey('agents.id'))
    condo_id = Column(Integer, ForeignKey('condo_basic.id'), nullable=True)
    
    # Relationships
    agent = relationship("Agent", back_populates="listings")
    condo = relationship("CondoBasic", back_populates="listings")
    favourited_by = relationship("UserFavourite", back_populates="listing", cascade="all, delete-orphan")
    
    # Match metadata
    match_score = Column(Float)
    match_method = Column(String(50))
