from sqlalchemy import Column, Integer, String, Text, ForeignKey, UniqueConstraint, DateTime, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.database import Base

class Agent(Base):
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
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = relationship("User", back_populates="agent_profile")
    listings = relationship("Listing", back_populates="agent")
    
    __table_args__ = (
        UniqueConstraint('name', 'mobile', name='uix_agent_name_mobile'),
    )
