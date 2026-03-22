from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CondoBase(BaseModel):
    condo_name: Optional[str] = None
    developer_name: Optional[str] = None
    street_name: Optional[str] = None
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    tenure: Optional[str] = None
    total_units: Optional[int] = None
    district: Optional[int] = None
    mrt_nearby: Optional[str] = None
    property_type: Optional[str] = None
    top_date: Optional[str] = None
    neighbourhood: Optional[str] = None
    num_floors: Optional[int] = None
    num_blocks: Optional[int] = None
    description: Optional[str] = None

class CondoResponse(CondoBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class CondoUnitBase(BaseModel):
    unit_number: Optional[str] = None
    floor_level: Optional[int] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    direction_facing: Optional[str] = None
    afternoon_sun: Optional[bool] = None
    unique_unit_description: Optional[str] = None
    is_penthouse: Optional[bool] = None
    size_sqm: Optional[float] = None
    price: Optional[float] = None
    listing_status: Optional[str] = None
    price_per_sqm: Optional[float] = None

class CondoUnitResponse(CondoUnitBase):
    unit_id: int
    condo_id: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class CondoUnitUpdate(CondoUnitBase):
    pass
