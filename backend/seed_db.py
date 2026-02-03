import asyncio
from sqlalchemy import select
from app.database import engine, Base, AsyncSessionLocal
from app.models.user import User
from app.models.agent import Agent
from app.models.listing import Listing
from app.utils.security import get_password_hash
from datetime import datetime

async def seed_data():
    async with AsyncSessionLocal() as session:
        # 1. Create Listings
        print("Seeding data...")
        
        # Check if Agent User exists
        stmt = select(User).where(User.email == "agent@example.com")
        result = await session.execute(stmt)
        agent_user = result.scalars().first()
        
        if not agent_user:
            password = get_password_hash("password123")
            agent_user = User(
                email="agent@example.com",
                hashed_password=password,
                full_name="James Bond",
                role="agent",
                is_active=True
            )
            session.add(agent_user)
            await session.commit()
            print("Created Agent User")
            
        # Refresh or re-select to be safe
        stmt = select(User).where(User.email == "agent@example.com")
        result = await session.execute(stmt)
        agent_user = result.scalars().first()
        agent_user_id = agent_user.id

        # Check if Agent Profile exists
        stmt = select(Agent).where(Agent.user_id == agent_user_id)
        result = await session.execute(stmt)
        agent_profile = result.scalars().first()
        
        if not agent_profile:
            agent_profile = Agent(
                user_id=agent_user_id,
                name=agent_user.full_name,
                mobile="91234567",
                cea="R123456G",
                description="Senior Agent at SgEstate Agency"
            )
            session.add(agent_profile)
            await session.commit()
            print("Created Agent Profile")
            
        # Re-select Agent to get ID
        stmt = select(Agent).where(Agent.user_id == agent_user_id)
        result = await session.execute(stmt)
        agent_profile = result.scalars().first()
        agent_id = agent_profile.id

        # Check Listings
        stmt = select(Listing).limit(1)
        result = await session.execute(stmt)
        if result.scalars().first():
            print("Listings already exist. Skipping.")
        else:
            listings = [
                Listing(
                    title="Luxury Condo in Orchard",
                    address="12 Orchard Turn, Singapore",
                    price=2500000,
                    display_price="$2.5M",
                    beds=3,
                    baths=2,
                    sqft=1200,
                    property_type="Condo",
                    district=9,
                    description="High floor with unblocked view. Walking distance to MRT.",
                    latitude=1.3039,
                    longitude=103.8320,
                    agent_id=agent_id,
                    buy_rent="Sale",
                    has_swimming_pool=True,
                    has_gym=True,
                    created_at=datetime.utcnow()
                ),
                Listing(
                    title="Spacious HDB in Tampines",
                    address="491 Tampines Street 45",
                    price=650000,
                    display_price="$650k",
                    beds=4,
                    baths=2,
                    sqft=1100,
                    property_type="HDB",
                    district=18,
                    description="Near schools and amenities. Corner unit.",
                    latitude=1.3611,
                    longitude=103.9531,
                    agent_id=agent_id,
                    buy_rent="Sale",
                    created_at=datetime.utcnow()
                ),
                Listing(
                    title="Modern Studio in River Valley",
                    address="88 River Valley Road",
                    price=3500,
                    display_price="$3,500/mo",
                    beds=1,
                    baths=1,
                    sqft=500,
                    property_type="Condo",
                    district=9,
                    description="Perfect for singles. Fully furnished.",
                    latitude=1.2938,
                    longitude=103.8427,
                    agent_id=agent_id,
                    buy_rent="Rent",
                    has_swimming_pool=True,
                    created_at=datetime.utcnow()
                ),
                Listing(
                    title="Landed Property with Pool",
                    address="15 Sentosa Cove",
                    price=8500000,
                    display_price="$8.5M",
                    beds=5,
                    baths=6,
                    sqft=4500,
                    property_type="Landed",
                    district=4,
                    description="Exclusive bungalow with private berth.",
                    latitude=1.2464,
                    longitude=103.8421,
                    agent_id=agent_id,
                    buy_rent="Sale",
                    has_swimming_pool=True,
                    has_parking=True,
                    created_at=datetime.utcnow()
                ),
                Listing(
                    title="Cozy 2-Bedder in Bishan",
                    address="18 Bishan Street 13",
                    price=4000,
                    display_price="$4,000/mo",
                    beds=2,
                    baths=2,
                    sqft=800,
                    property_type="Condo",
                    district=20,
                    description="Near Junction 8. High floor.",
                    latitude=1.3506,
                    longitude=103.8488,
                    agent_id=agent_id,
                    buy_rent="Rent",
                    has_gym=True,
                    created_at=datetime.utcnow()
                )
            ]
            
            for l in listings:
                session.add(l)
            
            await session.commit()
            print(f"Created {len(listings)} Listings")

if __name__ == "__main__":
    asyncio.run(seed_data())
