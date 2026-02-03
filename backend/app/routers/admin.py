from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from typing import List, Optional
import csv
import io
import codecs
from datetime import datetime

from app.database import get_db
from app.models.user import User, UserRole
from app.models.listing import Listing
from app.schemas.user import UserResponse, UserCreate
from app.schemas.listing import ListingCreate, ListingResponse
from app.services.auth import get_current_admin
from app.utils.security import get_password_hash
from app.services.listing import ListingService

router = APIRouter()

# --- User Management ---

@router.get("/users", response_model=List[UserResponse])
async def get_users(
    skip: int = 0,
    limit: int = 50,
    role: Optional[UserRole] = None,
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(get_current_admin)
):
    stmt = select(User)
    if role:
        stmt = stmt.where(User.role == role)
    stmt = stmt.offset(skip).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()

@router.put("/users/{user_id}/role")
async def update_user_role(
    user_id: int,
    role: UserRole,
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(get_current_admin)
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user.role = role
    await db.commit()
    return {"message": f"User role updated to {role}"}

# --- Batch Operations (Listings) ---

@router.post("/listings/import_csv")
async def import_listings_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(get_current_admin)
):
    """
    Batch import listings from CSV.
    Expected columns: title, price, beds, baths, sqft, address, property_type, district
    """
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV.")

    content = await file.read()
    # Decode parsing CSV
    csv_reader = csv.DictReader(codecs.iterdecode(io.BytesIO(content), 'utf-8'))
    
    success_count = 0
    errors = []
    
    # Simple predefined agent for imports/admin
    # In reality, might want to assign to specific agent or admin agent
    admin_agent_id = current_admin.id # Placeholder, should be agent_profile.id
    
    for row_idx, row in enumerate(csv_reader):
        try:
            # Basic validation & conversion
            listing_data = ListingCreate(
                title=row.get('title'),
                price=float(row.get('price')) if row.get('price') else 0,
                address=row.get('address'),
                beds=int(row.get('beds')) if row.get('beds') else 0,
                baths=int(row.get('baths')) if row.get('baths') else 0,
                sqft=int(row.get('sqft')) if row.get('sqft') else 0,
                property_type=row.get('property_type'),
                description=row.get('description', '')
            )
            
            # Create listing
            listing = Listing(**listing_data.dict(), agent_id=admin_agent_id)
            db.add(listing)
            success_count += 1
            
        except Exception as e:
            errors.append(f"Row {row_idx + 2}: {str(e)}")
            
    await db.commit()
    
    return {
        "message": f"Import complete. Imported {success_count} listings.",
        "errors": errors[:10] # Return first 10 errors
    }

# --- Stats Dashboard ---

@router.get("/stats")
async def get_dashboard_stats(
    db: AsyncSession = Depends(get_db),
    current_admin: User = Depends(get_current_admin)
):
    # Total Users
    user_count = await db.execute(select(func.count(User.id)))
    total_users = user_count.scalar()
    
    # Total Listings
    listing_count = await db.execute(select(func.count(Listing.id)))
    total_listings = listing_count.scalar()
    
    # Listings by Type (Group By)
    type_stats_q = select(Listing.property_type, func.count(Listing.id)).group_by(Listing.property_type)
    type_stats_res = await db.execute(type_stats_q)
    type_stats = {row[0]: row[1] for row in type_stats_res.all() if row[0]}
    
    return {
        "total_users": total_users,
        "total_listings": total_listings,
        "listings_by_type": type_stats,
        "generated_at": datetime.utcnow()
    }
