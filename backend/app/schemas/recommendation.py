from pydantic import BaseModel
from datetime import datetime
from typing import Optional
from app.schemas.listing import ListingResponse


class FavouriteResponse(BaseModel):
    id: int
    user_id: int
    listing_id: int
    created_at: datetime
    listing: Optional[ListingResponse] = None

    model_config = {"from_attributes": True}


class RecommendationResponse(BaseModel):
    listing: ListingResponse
    score: float                        # 0.0 – 1.0 weighted similarity
    match_reasons: list[str]            # e.g. ["Same district", "Similar price range"]
    valuation_estimate: Optional[int] = None  # XGBoost estimate in SGD, None if unavailable

    model_config = {"from_attributes": True}
