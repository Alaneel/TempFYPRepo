from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class HdbBase(BaseModel):
    block_number: str
    street_name: str
    town: str
    postal_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    total_floors: Optional[int] = None
    year_completed: Optional[int] = None
    has_residential: Optional[bool] = None
    has_commercial: Optional[bool] = None
    has_market_hawker: Optional[bool] = None
    has_multistorey_carpark: Optional[bool] = None
    has_void_deck: Optional[bool] = None
    total_dwelling_units: Optional[int] = None
    one_room_qty: Optional[int] = None
    two_room_qty: Optional[int] = None
    three_room_qty: Optional[int] = None
    four_room_qty: Optional[int] = None
    five_room_qty: Optional[int] = None

class HdbResponse(HdbBase):
    hdb_id: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
