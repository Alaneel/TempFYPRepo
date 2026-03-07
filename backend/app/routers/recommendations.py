"""
Favourites & Content-Based Recommendation Router
-------------------------------------------------
Endpoints:
  POST   /recommendations/favourites/{listing_id}   – add to favourites
  DELETE /recommendations/favourites/{listing_id}   – remove from favourites
  GET    /recommendations/favourites                 – list current user's favourites
  GET    /recommendations/favourites/{listing_id}/status – check if favourited
  GET    /recommendations/for-you                   – content-based recommendations
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import and_, func
from typing import List
import math

from app.database import get_db
from app.models.favourite import UserFavourite
from app.models.listing import Listing
from app.models.user import User
from app.schemas.recommendation import FavouriteResponse, RecommendationResponse
from app.schemas.listing import ListingResponse
from app.services.auth import get_current_active_user

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _price_similarity(p1: float, p2: float) -> float:
    """Exponential decay similarity: 1.0 when equal, ~0.5 at 30% difference."""
    if not p1 or not p2:
        return 0.0
    ratio = min(p1, p2) / max(p1, p2)
    return ratio ** 1.5


def _beds_similarity(b1: int, b2: int) -> float:
    """1.0 for exact match, decays by 0.25 per bedroom difference."""
    if b1 is None or b2 is None:
        return 0.0
    diff = abs(b1 - b2)
    return max(0.0, 1.0 - diff * 0.25)


def _score_candidate(candidate: Listing, profile: dict) -> tuple[float, list[str]]:
    """
    Compute weighted content-based similarity score between a candidate listing
    and the user preference profile derived from their favourites.

    Weights:
      property_type  0.30
      district       0.25
      price          0.25
      beds           0.20
    """
    score = 0.0
    reasons: list[str] = []

    # --- property_type (0.30) ---
    if profile["property_types"] and candidate.property_type:
        top_type, top_freq = max(profile["property_types"].items(), key=lambda x: x[1])
        total = sum(profile["property_types"].values())
        if candidate.property_type == top_type:
            weight = 0.30 * (top_freq / total)
            score += weight
            reasons.append(f"Matches your preferred property type ({top_type})")

    # --- district (0.25) ---
    if profile["districts"] and candidate.district is not None:
        top_dist, top_freq = max(profile["districts"].items(), key=lambda x: x[1])
        total = sum(profile["districts"].values())
        if candidate.district == top_dist:
            weight = 0.25 * (top_freq / total)
            score += weight
            reasons.append(f"Same district (D{top_dist})")
        elif candidate.district in profile["districts"]:
            # Partial credit for any favourited district
            freq = profile["districts"][candidate.district]
            score += 0.10 * (freq / total)
            reasons.append(f"District you've shown interest in (D{candidate.district})")

    # --- price (0.25) ---
    if profile["avg_price"] and candidate.price:
        sim = _price_similarity(profile["avg_price"], candidate.price)
        score += 0.25 * sim
        if sim > 0.8:
            reasons.append("Similar price range")

    # --- beds (0.20) ---
    if profile["avg_beds"] is not None and candidate.beds is not None:
        sim = _beds_similarity(round(profile["avg_beds"]), candidate.beds)
        score += 0.20 * sim
        if sim >= 0.75:
            reasons.append(f"Matches your preferred bedroom count ({candidate.beds} BR)")

    # --- buy_rent must match ---
    if profile["buy_rent"] and candidate.buy_rent != profile["buy_rent"]:
        score *= 0.1  # heavy penalty for wrong transaction mode

    return round(score, 4), reasons


def _build_preference_profile(favourites: list[UserFavourite]) -> dict:
    """Aggregate preference profile from a user's favourited listings."""
    if not favourites:
        return {}

    listings = [f.listing for f in favourites if f.listing is not None]
    if not listings:
        return {}

    # Frequency maps
    property_types: dict[str, int] = {}
    districts: dict[int, int] = {}
    prices: list[float] = []
    beds_list: list[int] = []
    buy_rents: dict[str, int] = {}

    for l in listings:
        if l.property_type:
            property_types[l.property_type] = property_types.get(l.property_type, 0) + 1
        if l.district is not None:
            districts[l.district] = districts.get(l.district, 0) + 1
        if l.price:
            prices.append(l.price)
        if l.beds is not None:
            beds_list.append(l.beds)
        if l.buy_rent:
            buy_rents[l.buy_rent] = buy_rents.get(l.buy_rent, 0) + 1

    return {
        "property_types": property_types,
        "districts": districts,
        "avg_price": sum(prices) / len(prices) if prices else None,
        "avg_beds": sum(beds_list) / len(beds_list) if beds_list else None,
        "buy_rent": max(buy_rents, key=buy_rents.get) if buy_rents else None,
        "favourited_ids": {f.listing_id for f in favourites},
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/favourites/{listing_id}", status_code=status.HTTP_201_CREATED)
async def add_favourite(
    listing_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a listing to the current user's favourites."""
    # Check listing exists
    result = await db.execute(select(Listing).where(Listing.id == listing_id))
    listing = result.scalar_one_or_none()
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    # Check not already favourited
    existing = await db.execute(
        select(UserFavourite).where(
            and_(UserFavourite.user_id == current_user.id,
                 UserFavourite.listing_id == listing_id)
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Already in favourites")

    fav = UserFavourite(user_id=current_user.id, listing_id=listing_id)
    db.add(fav)
    await db.commit()
    await db.refresh(fav)
    return {"message": "Added to favourites", "listing_id": listing_id}


@router.delete("/favourites/{listing_id}", status_code=status.HTTP_200_OK)
async def remove_favourite(
    listing_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a listing from the current user's favourites."""
    result = await db.execute(
        select(UserFavourite).where(
            and_(UserFavourite.user_id == current_user.id,
                 UserFavourite.listing_id == listing_id)
        )
    )
    fav = result.scalar_one_or_none()
    if not fav:
        raise HTTPException(status_code=404, detail="Favourite not found")

    await db.delete(fav)
    await db.commit()
    return {"message": "Removed from favourites", "listing_id": listing_id}


@router.get("/favourites/{listing_id}/status")
async def get_favourite_status(
    listing_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Check whether a specific listing is in the user's favourites."""
    result = await db.execute(
        select(UserFavourite).where(
            and_(UserFavourite.user_id == current_user.id,
                 UserFavourite.listing_id == listing_id)
        )
    )
    is_fav = result.scalar_one_or_none() is not None
    return {"listing_id": listing_id, "is_favourited": is_fav}


@router.get("/favourites", response_model=List[ListingResponse])
async def get_favourites(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the current user's favourited listings."""
    result = await db.execute(
        select(UserFavourite)
        .options(
            selectinload(UserFavourite.listing).selectinload(Listing.agent),
            selectinload(UserFavourite.listing).selectinload(Listing.condo),
        )
        .where(UserFavourite.user_id == current_user.id)
        .order_by(UserFavourite.created_at.desc())
    )
    favs = result.scalars().all()
    return [f.listing for f in favs if f.listing is not None]


@router.get("/for-you", response_model=List[RecommendationResponse])
async def get_recommendations(
    limit: int = 20,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Content-based recommendations derived from the user's favourites.

    Algorithm:
      1. Build a preference profile (property_type, district, price, beds, buy_rent)
         from the user's favourited listings.
      2. Score all active listings not already favourited using a weighted
         similarity function (property_type 30%, district 25%, price 25%, beds 20%).
      3. Return top-`limit` results sorted by descending score.
    """
    # Fetch user's favourites with listing details
    fav_result = await db.execute(
        select(UserFavourite)
        .options(selectinload(UserFavourite.listing))
        .where(UserFavourite.user_id == current_user.id)
    )
    favourites = fav_result.scalars().all()

    if not favourites:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No favourites found. Save some listings first to get personalised recommendations."
        )

    profile = _build_preference_profile(favourites)
    favourited_ids = profile.get("favourited_ids", set())

    # Fetch candidate listings (active, not already favourited)
    # Pre-filter by buy_rent and a broad price range (±60%) to limit DB load
    query = select(Listing).options(
        selectinload(Listing.agent),
        selectinload(Listing.condo),
    ).where(Listing.is_active == True)
    if profile.get("buy_rent"):
        query = query.where(Listing.buy_rent == profile["buy_rent"])
    if profile.get("avg_price"):
        low = profile["avg_price"] * 0.4
        high = profile["avg_price"] * 1.6
        query = query.where(Listing.price.between(low, high))

    candidates_result = await db.execute(query.limit(2000))
    candidates = candidates_result.scalars().all()

    # Score and rank
    scored: list[tuple[float, list[str], Listing]] = []
    for c in candidates:
        if c.id in favourited_ids:
            continue
        score, reasons = _score_candidate(c, profile)
        if score > 0:
            scored.append((score, reasons, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    top = scored[:limit]

    return [
        RecommendationResponse(
            listing=ListingResponse.model_validate(c),
            score=score,
            match_reasons=reasons,
        )
        for score, reasons, c in top
    ]
