"""
Favourites & Hybrid Recommendation Router
-----------------------------------------
Endpoints:
  POST   /recommendations/favourites/{listing_id}          – add to favourites
  DELETE /recommendations/favourites/{listing_id}          – remove from favourites
  GET    /recommendations/favourites/{listing_id}/status   – check if favourited
  GET    /recommendations/favourites                       – list favourites
  GET    /recommendations/for-you                          – hybrid recommendations

Recommendation algorithm (v2 – Hybrid Content-Based + Valuation-Grounded):
  Five scoring dimensions, weights summing to 1.0:
    property_type  0.25  – frequency-weighted type match
    district       0.20  – frequency-weighted district match
    price_sim      0.20  – exponential decay on avg favourite price
    beds           0.15  – bedroom count similarity
    bargain_score  0.20  – (XGBoost_estimate - listing_price) / XGBoost_estimate
                           normalised to [0,1]; positive = underpriced (good deal)

  The bargain_score dimension grounds the recommendation engine in the per-segment
  XGBoost valuation models, surfacing listings that are both preference-consistent
  AND potentially underpriced — a combination unavailable in standalone CF or CB
  systems, and a direct application of the valuation layer built in Chapter 4.
"""

import re
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import and_
from typing import List, Optional

from app.database import get_db
from app.models.favourite import UserFavourite
from app.models.listing import Listing
from app.models.user import User
from app.schemas.recommendation import RecommendationResponse
from app.schemas.listing import ListingResponse
from app.services.auth import get_current_active_user
import app.services.valuation as _val_svc

router = APIRouter()


# ---------------------------------------------------------------------------
# Scoring helpers
# ---------------------------------------------------------------------------

def _price_similarity(p1: float, p2: float) -> float:
    """Exponential decay: 1.0 when equal, ~0.5 at 30% difference."""
    if not p1 or not p2:
        return 0.0
    ratio = min(p1, p2) / max(p1, p2)
    return ratio ** 1.5


def _beds_similarity(b1: int, b2: int) -> float:
    """1.0 for exact match, -0.25 per bedroom difference, floor 0."""
    if b1 is None or b2 is None:
        return 0.0
    return max(0.0, 1.0 - abs(b1 - b2) * 0.25)


def _get_bargain_score(candidate: Listing) -> tuple[float, Optional[float]]:
    """
    Call the XGBoost valuation service synchronously (models are in-process).

    Returns:
        (normalised_bargain_score [0,1], estimated_price_or_None)

    bargain_raw = (estimate - price) / estimate
        Clamped to [-0.5, +0.5], then shifted to [0, 1]:
            0.0 = overpriced by >=50%
            0.5 = at market estimate
            1.0 = underpriced by >=50%
    """
    if not (candidate.price and candidate.sqft and candidate.beds
            and candidate.property_type and candidate.buy_rent):
        return 0.5, None  # neutral when data missing

    try:
        built_year = None
        if candidate.built_year:
            m = re.search(r'\d{4}', str(candidate.built_year))
            if m:
                built_year = int(m.group())

        result = _val_svc.estimate(
            property_type=candidate.property_type,
            buy_rent=candidate.buy_rent,
            beds=float(candidate.beds),
            sqft=float(candidate.sqft),
            tenure=candidate.tenure,
            built_year=built_year,
            district=candidate.district,
        )
        estimate = result.get("estimate")
        if not estimate or estimate <= 0:
            return 0.5, None

        raw = (estimate - candidate.price) / estimate
        clamped = max(-0.5, min(0.5, raw))
        normalised = round(clamped + 0.5, 4)  # shift [-0.5,0.5] → [0,1]
        return normalised, float(estimate)

    except Exception:
        return 0.5, None  # neutral on any error


def _score_candidate(
    candidate: Listing, profile: dict
) -> tuple[float, list[str], Optional[float]]:
    """
    Hybrid content-based + valuation-grounded scoring.

    Weights: type 0.25 | district 0.20 | price_sim 0.20 | beds 0.15 | bargain 0.20
    Returns: (total_score, match_reasons, valuation_estimate)
    """
    score = 0.0
    reasons: list[str] = []

    # property_type (0.25)
    if profile["property_types"] and candidate.property_type:
        top_type, top_freq = max(profile["property_types"].items(), key=lambda x: x[1])
        total = sum(profile["property_types"].values())
        if candidate.property_type == top_type:
            score += 0.25 * (top_freq / total)
            reasons.append(f"Matches your preferred property type ({top_type})")

    # district (0.20)
    if profile["districts"] and candidate.district is not None:
        top_dist, top_freq = max(profile["districts"].items(), key=lambda x: x[1])
        total = sum(profile["districts"].values())
        if candidate.district == top_dist:
            score += 0.20 * (top_freq / total)
            reasons.append(f"Same district (D{top_dist})")
        elif candidate.district in profile["districts"]:
            freq = profile["districts"][candidate.district]
            score += 0.08 * (freq / total)
            reasons.append(f"District you've shown interest in (D{candidate.district})")

    # price similarity (0.20)
    if profile["avg_price"] and candidate.price:
        sim = _price_similarity(profile["avg_price"], candidate.price)
        score += 0.20 * sim
        if sim > 0.8:
            reasons.append("Similar price range")

    # beds (0.15)
    if profile["avg_beds"] is not None and candidate.beds is not None:
        sim = _beds_similarity(round(profile["avg_beds"]), candidate.beds)
        score += 0.15 * sim
        if sim >= 0.75:
            reasons.append(f"Matches your preferred bedroom count ({candidate.beds} BR)")

    # bargain_score (0.20) — valuation-grounded
    bargain_norm, valuation_estimate = _get_bargain_score(candidate)
    score += 0.20 * bargain_norm
    if valuation_estimate and candidate.price:
        pct = round((valuation_estimate - candidate.price) / valuation_estimate * 100, 1)
        if pct >= 5.0:
            reasons.append(f"Estimated {pct}% below market valuation")
        elif pct <= -10.0:
            reasons.append(f"Listed {abs(pct)}% above model estimate")

    # buy_rent must match (hard penalty)
    if profile["buy_rent"] and candidate.buy_rent != profile["buy_rent"]:
        score *= 0.1

    return round(score, 4), reasons, valuation_estimate


def _build_preference_profile(favourites: list) -> dict:
    """Aggregate preference profile from a user's favourited listings."""
    if not favourites:
        return {}

    listings = [f.listing for f in favourites if f.listing is not None]
    if not listings:
        return {}

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
    result = await db.execute(select(Listing).where(Listing.id == listing_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Listing not found")

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
    return {"message": "Added to favourites", "listing_id": listing_id}


@router.delete("/favourites/{listing_id}", status_code=status.HTTP_200_OK)
async def remove_favourite(
    listing_id: int,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
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
    result = await db.execute(
        select(UserFavourite).where(
            and_(UserFavourite.user_id == current_user.id,
                 UserFavourite.listing_id == listing_id)
        )
    )
    return {"listing_id": listing_id, "is_favourited": result.scalar_one_or_none() is not None}


@router.get("/favourites", response_model=List[ListingResponse])
async def get_favourites(
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
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
    Hybrid content-based + valuation-grounded recommendations.

    1. Build preference profile from user's favourites.
    2. Pre-filter candidates by buy/rent mode and ±60% price band.
    3. Score each candidate on 5 dimensions (type/district/price/beds/bargain).
    4. Return top-limit results sorted by descending score.
    """
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

    # Pre-filter candidates
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
    scored = []
    for c in candidates:
        if c.id in favourited_ids:
            continue
        score, reasons, valuation_estimate = _score_candidate(c, profile)
        if score > 0:
            scored.append((score, reasons, valuation_estimate, c))

    scored.sort(key=lambda x: x[0], reverse=True)

    return [
        RecommendationResponse(
            listing=ListingResponse.model_validate(c),
            score=score,
            match_reasons=reasons,
            valuation_estimate=int(ve) if ve else None,
        )
        for score, reasons, ve, c in scored[:limit]
    ]
