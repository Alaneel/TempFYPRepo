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
    db: AsyncSession = Depends(get_db)
):
    # Generate Cache Key
    params = {
        "page": page, "limit": limit, "min_price": min_price, "max_price": max_price,
        "beds": beds, "property_type": property_type, "buy_rent": buy_rent, "district": district, 
        "agent_id": agent_id, "q": q,
        "min_lat": min_lat, "max_lat": max_lat, "min_lng": min_lng, "max_lng": max_lng
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
        max_lng=max_lng
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
