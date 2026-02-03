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
