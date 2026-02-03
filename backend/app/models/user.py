from sqlalchemy import Column, Integer, String, Float, Text, Boolean, DateTime, ForeignKey, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from app.database import Base

class UserRole(str, enum.Enum):
    ADMIN = "admin"
    AGENT = "agent"
    CUSTOMER = "customer"

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String, index=True)
    role = Column(String, default=UserRole.CUSTOMER, index=True) # admin, agent, customer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    agent_profile = relationship("Agent", back_populates="user", uselist=False)
    # favorites = relationship("Listing", secondary="user_favorites")
