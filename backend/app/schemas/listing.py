from pydantic import BaseModel, computed_field
from typing import Optional
from datetime import datetime
import re
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
    latitude: Optional[float] = None
    longitude: Optional[float] = None

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
        
        # Return relative paths — frontend resolves them against its own origin,
        # so this works correctly in both local dev and production deployment.
        if 'hdb' in ptype:
            return f"/placeholders/hdb_{seed % 4}.png"
        elif 'landed' in ptype or 'bungalow' in ptype or 'terrace' in ptype:
            return f"/placeholders/landed_{seed % 4}.png"
        else: # Condominiums / Others
            return f"/placeholders/condo_{seed % 2}.png"

    @computed_field
    def lease_risk_tier(self) -> Optional[dict]:
        """CPF lease-eligibility risk tier for HDB listings only.
        
        Based on CPF Board rules:
        - >60 yrs remaining: full CPF OA + HDB Concessionary Loan eligible
        - 30-60 yrs: CPF OA usage restricted; commercial bank loan only
        - <30 yrs: no CPF OA usage; no HDB mortgage eligible
        """
        ptype = str(self.property_type).lower() if self.property_type else ""
        if 'hdb' not in ptype:
            return None
        if not self.built_year:
            return None
        try:
            year_str = re.sub(r'\D', '', str(self.built_year))[:4]
            built = int(year_str)
            if built < 1960 or built > 2026:
                return None
        except (ValueError, TypeError):
            return None

        remaining = 99 - (2026 - built)
        if remaining <= 0:
            return None

        if remaining > 60:
            return {
                "tier": "green",
                "label": "Full CPF Eligible",
                "tooltip": f"{remaining} yrs remaining · Full CPF OA usage and HDB Concessionary Loan eligible",
                "remaining_years": remaining,
            }
        elif remaining > 30:
            return {
                "tier": "amber",
                "label": "CPF Restricted",
                "tooltip": f"{remaining} yrs remaining · CPF OA usage restricted; only commercial bank loans at reduced quantum",
                "remaining_years": remaining,
            }
        else:
            return {
                "tier": "red",
                "label": "CPF Ineligible",
                "tooltip": f"{remaining} yrs remaining · No CPF Ordinary Account usage; no HDB Concessionary Loan eligible",
                "remaining_years": remaining,
            }
    
    class Config:
        from_attributes = True
