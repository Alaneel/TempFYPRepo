import asyncio
from app.database import engine, Base
# Import all models to ensure they are registered with Base
from app.models.user import User
from app.models.agent import Agent
from app.models.listing import Listing
from app.models.condo import CondoBasic

async def init_db():
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables initialized successfully!")

if __name__ == "__main__":
    asyncio.run(init_db())
