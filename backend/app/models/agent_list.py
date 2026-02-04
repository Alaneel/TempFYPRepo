"""
AgentList model for storing additional agent information from CEA registry.
Joins with agents table via cea_number field.
"""
from sqlalchemy import Column, BigInteger, String, Date, DateTime
from datetime import datetime
from app.database import Base


class AgentList(Base):
    """
    Agent list table - contains detailed agent information from CEA registry.
    Can be joined with agents table via cea_number = agents.cea
    """
    __tablename__ = 'agent_list'
    
    id = Column(BigInteger, primary_key=True)
    cea_number = Column(String(8), index=True, nullable=True)
    agent_name = Column(String(100), nullable=True)
    phone = Column(String(20), nullable=True)
    company_name = Column(String(100), nullable=True)
    agency_license = Column(String(9), nullable=True)
    license_expiry = Column(Date, nullable=True)
    registration_date = Column(Date, nullable=True)
    photo_url = Column(String(255), nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
