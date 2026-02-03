from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import or_, func
from app.models.listing import Listing
from app.schemas.listing import ListingCreate, ListingUpdate
from typing import List, Optional

class ListingService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_listings(
        self, 
        skip: int = 0, 
        limit: int = 20,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        beds: Optional[int] = None,
        property_type: Optional[str] = None,
        buy_rent: Optional[str] = None,
        district: Optional[int] = None,
        agent_id: Optional[int] = None,
        query: Optional[str] = None,
        min_lat: Optional[float] = None,
        max_lat: Optional[float] = None,
        min_lng: Optional[float] = None,
        max_lng: Optional[float] = None
    ) -> List[Listing]:
        stmt = select(Listing).options(selectinload(Listing.agent), selectinload(Listing.condo))
        
        # Filters
        if min_price is not None:
            stmt = stmt.where(Listing.price >= min_price)
        if max_price is not None:
            stmt = stmt.where(Listing.price <= max_price)
        if beds is not None:
            stmt = stmt.where(Listing.beds >= beds)
        if property_type:
            stmt = stmt.where(Listing.property_type.ilike(f"%{property_type}%"))
        if buy_rent:
             stmt = stmt.where(Listing.buy_rent.ilike(f"%{buy_rent}%"))
        if district is not None:
            stmt = stmt.where(Listing.district == district)
        if agent_id is not None:
            stmt = stmt.where(Listing.agent_id == agent_id)
            
        # Geospatial Filters
        if min_lat is not None:
            stmt = stmt.where(Listing.latitude >= min_lat)
        if max_lat is not None:
            stmt = stmt.where(Listing.latitude <= max_lat)
        if min_lng is not None:
            stmt = stmt.where(Listing.longitude >= min_lng)
        if max_lng is not None:
            stmt = stmt.where(Listing.longitude <= max_lng)
            
        # Search
        if query:
            search_query = f"%{query}%"
            stmt = stmt.where(
                or_(
                    Listing.title.ilike(search_query),
                    Listing.address.ilike(search_query),
                    Listing.description.ilike(search_query)
                )
            )
            
        # Pagination
        stmt = stmt.offset(skip).limit(limit)
        
        result = await self.db.execute(stmt)
        return result.scalars().all()

    async def get_total_count(
        self,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        beds: Optional[int] = None,
        property_type: Optional[str] = None,
        buy_rent: Optional[str] = None,
        district: Optional[int] = None,
        agent_id: Optional[int] = None,
        query: Optional[str] = None,
        min_lat: Optional[float] = None,
        max_lat: Optional[float] = None,
        min_lng: Optional[float] = None,
        max_lng: Optional[float] = None
    ) -> int:
        stmt = select(func.count()).select_from(Listing)
        
        # Filters (Must match get_listings logic)
        if min_price is not None:
            stmt = stmt.where(Listing.price >= min_price)
        if max_price is not None:
            stmt = stmt.where(Listing.price <= max_price)
        if beds is not None:
            stmt = stmt.where(Listing.beds >= beds)
        if property_type:
            stmt = stmt.where(Listing.property_type.ilike(f"%{property_type}%"))
        if buy_rent:
             stmt = stmt.where(Listing.buy_rent.ilike(f"%{buy_rent}%"))
        if district is not None:
            stmt = stmt.where(Listing.district == district)
        if agent_id is not None:
            stmt = stmt.where(Listing.agent_id == agent_id)
            
        # Geospatial Filters
        if min_lat is not None:
            stmt = stmt.where(Listing.latitude >= min_lat)
        if max_lat is not None:
            stmt = stmt.where(Listing.latitude <= max_lat)
        if min_lng is not None:
            stmt = stmt.where(Listing.longitude >= min_lng)
        if max_lng is not None:
            stmt = stmt.where(Listing.longitude <= max_lng)
            
        # Search
        if query:
            search_query = f"%{query}%"
            stmt = stmt.where(
                or_(
                    Listing.title.ilike(search_query),
                    Listing.address.ilike(search_query),
                    Listing.description.ilike(search_query)
                )
            )

        result = await self.db.execute(stmt)
        return result.scalar()

    async def get_listing(self, listing_id: int) -> Optional[Listing]:
        result = await self.db.execute(select(Listing).options(selectinload(Listing.agent), selectinload(Listing.condo)).where(Listing.id == listing_id))
        return result.scalar_one_or_none()

    async def create_listing(self, listing: ListingCreate, agent_id: int) -> Listing:
        db_listing = Listing(**listing.dict(), agent_id=agent_id)
        self.db.add(db_listing)
        await self.db.commit()
        await self.db.refresh(db_listing)
        return db_listing

    async def update_listing(self, listing_id: int, listing: ListingUpdate, agent_id: int) -> Optional[Listing]:
        db_listing = await self.get_listing(listing_id)
        if not db_listing:
            return None
        
        # Ensure ownership
        if db_listing.agent_id != agent_id:
            # We will handle admin override in controller
            return None
            
        update_data = listing.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_listing, key, value)
            
        await self.db.commit()
        await self.db.refresh(db_listing)
        return db_listing

    async def delete_listing(self, listing_id: int, agent_id: int) -> bool:
        db_listing = await self.get_listing(listing_id)
        if not db_listing:
            return False
            
        if db_listing.agent_id != agent_id:
            return False
            
        await self.db.delete(db_listing)
        await self.db.commit()
        return True
