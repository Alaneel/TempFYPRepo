from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

class AgentBase(BaseModel):
    name: Optional[str] = None
    mobile: Optional[str] = None
    cea: Optional[str] = None
    rating: Optional[float] = None
    description: Optional[str] = None
    url: Optional[str] = None
    source_id: Optional[str] = None

class AgentCreate(AgentBase):
    pass
    
class AgentUpdate(AgentBase):
    pass

class AgentResponse(AgentBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    # Fields from agent_list table (joined via cea)
    company_name: Optional[str] = None
    agency_license: Optional[str] = None
    license_expiry: Optional[date] = None
    registration_date: Optional[date] = None
    photo_url: Optional[str] = None
    listing_count: Optional[int] = None
    
    class Config:
        from_attributes = True
