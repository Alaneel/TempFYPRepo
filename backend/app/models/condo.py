from sqlalchemy import Column, Integer, String, Float, Text, Boolean, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class CondoBasic(Base):
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    listings = relationship("Listing", back_populates="condo")
