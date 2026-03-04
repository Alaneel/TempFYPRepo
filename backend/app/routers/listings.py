from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import hashlib
import json

from app.database import get_db
from app.schemas.listing import ListingResponse, ListingCreate, ListingUpdate
from app.schemas.common import PaginatedResponse
from app.services.listing import ListingService
from app.services.auth import get_current_user, get_current_agent, get_current_admin
from app.services.cache import cache
from app.models.user import User
from app.services.semantic_search import parse_nl_query

router = APIRouter()

@router.get("/", response_model=PaginatedResponse[ListingResponse])
async def get_listings(
    page: int = 1,
    limit: int = 20,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    beds: Optional[int] = None,
    property_type: Optional[str] = None,
    buy_rent: Optional[str] = None,
    district: Optional[int] = None,
    agent_id: Optional[int] = None,
    q: Optional[str] = None,
    min_lat: Optional[float] = None,
    max_lat: Optional[float] = None,
    min_lng: Optional[float] = None,
    max_lng: Optional[float] = None,
    sort_by: Optional[str] = Query(None, description="Sort order: recommended, price_asc, price_desc, newest"),
    db: AsyncSession = Depends(get_db)
):
    # Generate Cache Key
    params = {
        "page": page, "limit": limit, "min_price": min_price, "max_price": max_price,
        "beds": beds, "property_type": property_type, "buy_rent": buy_rent, "district": district, 
        "agent_id": agent_id, "q": q,
        "min_lat": min_lat, "max_lat": max_lat, "min_lng": min_lng, "max_lng": max_lng,
        "sort_by": sort_by
    }
    params_str = json.dumps(params, sort_keys=True)
    cache_key = f"listings:{hashlib.md5(params_str.encode()).hexdigest()}"
    
    # Check Cache
    cached_data = await cache.get(cache_key)
    if cached_data:
        return cached_data
        
    service = ListingService(db)
    skip = (page - 1) * limit
    
    listings = await service.get_listings(
        skip=skip, 
        limit=limit,
        min_price=min_price,
        max_price=max_price,
        beds=beds,
        property_type=property_type,
        buy_rent=buy_rent,
        district=district,
    
        agent_id=agent_id,
        query=q,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng,
        sort_by=sort_by
    )
    
    total = await service.get_total_count(
        min_price=min_price,
        max_price=max_price,
        beds=beds,
        property_type=property_type,
        buy_rent=buy_rent,
        district=district,
        agent_id=agent_id,
        query=q,
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng
    )
    
    response_data = {
        "total": total,
        "page": page,
        "limit": limit,
        "data": jsonable_encoder(listings)
    }
    
    # Save to Cache
    await cache.set(cache_key, response_data, ttl=120) # 2 mins TTL
    
    return response_data

@router.get("/semantic-search", response_model=PaginatedResponse[ListingResponse])
async def semantic_search_listings(
    q: str = Query(..., description="Natural language search query"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    sort_by: Optional[str] = Query(None, description="Sort order: recommended, price_asc, price_desc, newest"),
    db: AsyncSession = Depends(get_db)
):
    """
    Search property listings using natural language.

    Examples:
    - "3 bedroom condo near Tampines MRT, budget 1.2 million, freehold"
    - "cheap HDB for rent in Bishan under 2500 per month"
    - "4BR landed property Bukit Timah freehold"

    The query is parsed by Claude AI into structured filters,
    then applied to the listings database.
    """
    # 1. Parse natural language → structured filters
    try:
        filters = parse_nl_query(q)
    except Exception as e:
        raise HTTPException(
            status_code=422,
            detail=f"Failed to parse query: {str(e)}"
        )

    # 2. Query database using existing ListingService with progressive fallback
    service = ListingService(db)
    skip = (page - 1) * limit

    # Define fallback stages: progressively relax filters when 0 results
    # Stage 0: all filters (original)
    # Stage 1: remove price constraints
    # Stage 2: remove price + tenure
    # Stage 3: remove price + tenure + district (keyword/type only)
    fallback_stages = [
        # (remove_price, remove_tenure, remove_district, label)
        (False, False, False, None),
        (True,  False, False, "price filter removed — showing results in any price range"),
        (True,  True,  False, "price & tenure filters removed — showing broader results"),
        (True,  True,  True,  "price, tenure & district filters removed — showing all matching property types"),
    ]

    active_filters = filters.copy()
    fallback_label: Optional[str] = None

    for remove_price, remove_tenure, remove_district, label in fallback_stages:
        query_filters = filters.copy()
        if remove_price:
            query_filters.pop("min_price", None)
            query_filters.pop("max_price", None)
        if remove_tenure:
            query_filters.pop("tenure", None)
        if remove_district:
            query_filters.pop("district", None)

        total = await service.get_total_count(
            min_price=query_filters.get("min_price"),
            max_price=query_filters.get("max_price"),
            beds=query_filters.get("beds"),
            property_type=query_filters.get("property_type"),
            buy_rent=query_filters.get("buy_rent"),
            district=query_filters.get("district"),
            tenure=query_filters.get("tenure"),
            query=query_filters.get("query"),
        )

        if total > 0:
            active_filters = query_filters
            fallback_label = label
            break

    listings = await service.get_listings(
        skip=skip,
        limit=limit,
        min_price=active_filters.get("min_price"),
        max_price=active_filters.get("max_price"),
        beds=active_filters.get("beds"),
        property_type=active_filters.get("property_type"),
        buy_rent=active_filters.get("buy_rent"),
        district=active_filters.get("district"),
        tenure=active_filters.get("tenure"),
        query=active_filters.get("query"),
        sort_by=sort_by
    )

    response: dict = {
        "total": total,
        "page": page,
        "limit": limit,
        "data": jsonable_encoder(listings),
        # Include parsed filters in response for frontend to display
        "_parsed_filters": active_filters,
        "_original_query": q,
    }
    if fallback_label:
        response["_fallback_notice"] = fallback_label

    return response

@router.get("/{listing_id}", response_model=ListingResponse)
async def get_listing(
    listing_id: int,
    db: AsyncSession = Depends(get_db)
):
    cache_key = f"listing:{listing_id}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    service = ListingService(db)
    listing = await service.get_listing(listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
        
    data = jsonable_encoder(listing)
    await cache.set(cache_key, data, ttl=300) # 5 mins TTL
    return data

@router.get("/me/all", response_model=PaginatedResponse[ListingResponse])
async def get_my_listings(
    page: int = 1,
    limit: int = 20,
    current_user: User = Depends(get_current_agent),
    db: AsyncSession = Depends(get_db)
):
    """
    Get listings for the current logged-in agent.
    """
    # 1. Get Agent ID linked to User
    from app.models.agent import Agent
    from sqlalchemy import select
    
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    if not agent:
        # If no agent profile, return empty
        return {
            "total": 0,
            "page": page,
            "limit": limit,
            "data": []
        }
        
    service = ListingService(db)
    skip = (page - 1) * limit
    
    # Reuse get_listings but force agent_id
    listings = await service.get_listings(
        skip=skip, 
        limit=limit,
        agent_id=agent.id
    )
    
    total = await service.get_total_count(
        agent_id=agent.id
    )
    
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "data": jsonable_encoder(listings)
    }

# --- Agent/Admin Protected Routes ---

@router.post("/", response_model=ListingResponse)
async def create_listing(
    listing: ListingCreate,
    current_user: User = Depends(get_current_agent),
    db: AsyncSession = Depends(get_db)
):
    # Resolve Agent ID
    from app.models.agent import Agent
    from sqlalchemy import select
    
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=400, detail="Agent profile not found. Please complete onboarding first.")
        
    service = ListingService(db)
    new_listing = await service.create_listing(listing, agent_id=agent.id)
    
    # Manually attach agent to avoid lazy load error
    new_listing.agent = agent
    
    # Invalidate Cache
    await cache.clear_listings_cache()
    
    return new_listing

@router.put("/{listing_id}", response_model=ListingResponse)
async def update_listing(
    listing_id: int,
    listing: ListingUpdate,
    current_user: User = Depends(get_current_agent),
    db: AsyncSession = Depends(get_db)
):
    # Resolve Agent ID
    from app.models.agent import Agent
    from sqlalchemy import select
    
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=400, detail="Agent profile not found")

    service = ListingService(db)
    updated = await service.update_listing(listing_id, listing, agent_id=agent.id)
    if not updated:
        raise HTTPException(status_code=403, detail="Not authorized or listing not found")
        
    # Invalidate Cache (List and Detail)
    await cache.delete(f"listing:{listing_id}")
    await cache.clear_listings_cache()
    
    return updated

@router.delete("/{listing_id}")
async def delete_listing(
    listing_id: int,
    current_user: User = Depends(get_current_agent),
    db: AsyncSession = Depends(get_db)
):
    # Resolve Agent ID
    from app.models.agent import Agent
    from sqlalchemy import select
    
    result = await db.execute(select(Agent).where(Agent.user_id == current_user.id))
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=400, detail="Agent profile not found")

    service = ListingService(db)
    deleted = await service.delete_listing(listing_id, agent_id=agent.id)
    if not deleted:
        raise HTTPException(status_code=403, detail="Not authorized or listing not found")
        
    # Invalidate Cache
    await cache.delete(f"listing:{listing_id}")
    await cache.clear_listings_cache()
    
    return {"message": "Listing deleted successfully"}