import asyncio
import os
import sys
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select

# Add the backend directory to the sys.path to resolve imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.models.user import User
from app.utils.security import get_password_hash

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "real_estate_app")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgres")

# Note the +asyncpg for async operations
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

async def seed_users():
    print(f"Connecting to: {DATABASE_URL}")
    engine = create_async_engine(DATABASE_URL, echo=False)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session() as session:
        # Default test users
        users_to_seed = [
            {
                "email": "admin@sgestate.com",
                "password": "admin",
                "full_name": "Admin User",
                "role": "admin"
            },
            {
                "email": "agent@sgestate.com",
                "password": "agent",
                "full_name": "Test Agent",
                "role": "agent"
            },
            {
                "email": "user@sgestate.com",
                "password": "user",
                "full_name": "John Doe",
                "role": "customer"
            }
        ]

        # Check and insert each user
        for user_data in users_to_seed:
            result = await session.execute(select(User).where(User.email == user_data["email"]))
            existing_user = result.scalar_one_or_none()

            if existing_user:
                print(f"User {user_data['email']} already exists. Skipping.")
            else:
                hashed_pw = get_password_hash(user_data["password"])
                new_user = User(
                    email=user_data["email"],
                    hashed_password=hashed_pw,
                    full_name=user_data["full_name"],
                    role=user_data["role"]
                )
                session.add(new_user)
                print(f"Created user: {user_data['email']} ({user_data['role']})")

        await session.commit()
    
    await engine.dispose()
    print("Seed complete.")

if __name__ == "__main__":
    asyncio.run(seed_users())
