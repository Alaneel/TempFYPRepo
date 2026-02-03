import asyncio
from app.database import engine, Base
# Import all models
from app.models.user import User
from app.models.agent import Agent
from app.models.listing import Listing
from app.models.condo import CondoBasic

async def reset_db():
    async with engine.begin() as conn:
        print("Dropping all tables...")
        await conn.run_sync(Base.metadata.drop_all)
        print("Creating all tables...")
        await conn.run_sync(Base.metadata.create_all)
    print("Database reset successfully!")

if __name__ == "__main__":
    asyncio.run(reset_db())
