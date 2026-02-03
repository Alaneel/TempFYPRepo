from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from datetime import datetime
from app.schemas.agent import AgentResponse
from app.schemas.condo import CondoResponse

class ListingBase(BaseModel):
    title: str
    address: Optional[str] = None
    display_price: Optional[str] = None
    price: Optional[float] = None
    beds: Optional[int] = None
    baths: Optional[int] = None
    sqft: Optional[int] = None
    property_type: Optional[str] = None
    district: Optional[int] = None
    neighbourhood: Optional[str] = None
    description: Optional[str] = None
    buy_rent: Optional[str] = None
    has_swimming_pool: Optional[bool] = False
    has_gym: Optional[bool] = False
    
    # Missing Fields Added
    built_year: Optional[str] = None
    tenure: Optional[str] = None
    psf: Optional[float] = None
    display_psf: Optional[str] = None
    amenities_json: Optional[str] = None
    facilities_json: Optional[str] = None

class ListingCreate(ListingBase):
    pass

class ListingUpdate(ListingBase):
    title: Optional[str] = None # Make title optional for update

class ListingResponse(ListingBase):
    id: int
    agent_id: int
    condo_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime
    
    # Details
    url: Optional[str] = None
    source: Optional[str] = None
    match_score: Optional[float] = None
    
    # Relationships
    agent: Optional[AgentResponse] = None
    condo: Optional[CondoResponse] = None
    
    class Config:
        from_attributes = True
