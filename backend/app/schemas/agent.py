from pydantic import BaseModel
from typing import Optional
from datetime import datetime

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
    
    class Config:
        from_attributes = True
