from pydantic import BaseModel, computed_field
from typing import Optional
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
    
    # We add an image_url property to ensure the UI does not display broken images.
    @computed_field
    def image_url(self) -> str:
        # We can seed the image selection with the property's ID so that 
        # the same property always gets the exact same placeholder image.
        seed = self.id % 5
        
        ptype = str(self.property_type).lower() if self.property_type else ""
        
        # Using local Next.js static asset URLs to avoid 404s from external hosts
        frontend_url = "http://localhost:3000"
        
        if 'hdb' in ptype:
            return f"{frontend_url}/placeholders/hdb_{seed % 4}.png"
        elif 'landed' in ptype or 'bungalow' in ptype or 'terrace' in ptype:
            return f"{frontend_url}/placeholders/landed_{seed % 4}.png"
        else: # Condominiums / Others
            return f"{frontend_url}/placeholders/condo_{seed % 2}.png"
    
    class Config:
        from_attributes = True
