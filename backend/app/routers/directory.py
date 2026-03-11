from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, func
from typing import List, Optional

from app.database import get_db
from app.models.condo import CondoBasic
from app.models.hdb import HdbBasic
from app.schemas.condo import CondoResponse
from app.schemas.hdb import HdbResponse
from app.schemas.listing import ListingResponse
from app.schemas.common import PaginatedResponse
from app.services.listing import ListingService

router = APIRouter()

@router.get("/search")
async def search_directory(
    q: str = Query(..., description="Search query for Condo name, HDB block, or street"),
    limit: int = Query(20, le=100),
    db: AsyncSession = Depends(get_db)
):
    """
    Search across both Condo and HDB master directories.
    Returns a unified list of matched properties.
    """
    search_term = f"%{q.lower()}%"
    
    # Search Condos
    condo_stmt = select(CondoBasic).where(
        or_(
            func.lower(CondoBasic.condo_name).like(search_term),
            func.lower(CondoBasic.street_name).like(search_term)
        )
    ).limit(limit)
    condo_result = await db.execute(condo_stmt)
    condos = condo_result.scalars().all()
    
    # Search HDBs
    hdb_stmt = select(HdbBasic).where(
        or_(
            func.lower(HdbBasic.block_number + ' ' + HdbBasic.street_name).like(search_term),
            func.lower(HdbBasic.street_name).like(search_term),
            func.lower(HdbBasic.town).like(search_term)
        )
    ).limit(limit)
    hdb_result = await db.execute(hdb_stmt)
    hdbs = hdb_result.scalars().all()
    
    return {
        "condos": [CondoResponse.model_validate(c) for c in condos],
        "hdbs": [HdbResponse.model_validate(h) for h in hdbs]
    }

@router.get("/condos/{condo_id}", response_model=CondoResponse)
async def get_condo(
    condo_id: int,
    db: AsyncSession = Depends(get_db)
):
    stmt = select(CondoBasic).where(CondoBasic.id == condo_id)
    result = await db.execute(stmt)
    condo = result.scalar_one_or_none()
    if not condo:
        raise HTTPException(status_code=404, detail="Condo not found")
    return condo

@router.get("/hdbs/{hdb_id}", response_model=HdbResponse)
async def get_hdb(
    hdb_id: int,
    db: AsyncSession = Depends(get_db)
):
    stmt = select(HdbBasic).where(HdbBasic.hdb_id == hdb_id)
    result = await db.execute(stmt)
    hdb = result.scalar_one_or_none()
    if not hdb:
        raise HTTPException(status_code=404, detail="HDB block not found")
    return hdb

@router.get("/condos/{condo_id}/listings", response_model=PaginatedResponse[ListingResponse])
async def get_condo_listings(
    condo_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    service = ListingService(db)
    skip = (page - 1) * limit
    listings = await service.get_listings(skip=skip, limit=limit, condo_id=condo_id)
    total = await service.get_total_count(condo_id=condo_id)
    return {"total": total, "page": page, "limit": limit, "data": listings}

@router.get("/hdbs/{hdb_id}/listings", response_model=PaginatedResponse[ListingResponse])
async def get_hdb_listings(
    hdb_id: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    service = ListingService(db)
    skip = (page - 1) * limit
    listings = await service.get_listings(skip=skip, limit=limit, hdb_id=hdb_id)
    total = await service.get_total_count(hdb_id=hdb_id)
    return {"total": total, "page": page, "limit": limit, "data": listings}
