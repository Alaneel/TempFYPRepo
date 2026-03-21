import sys
import os
import asyncio
from pathlib import Path

# Add backend to path so we can import the exact routing logic used in API
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.database import AsyncSessionLocal
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from app.models.listing import Listing
from app.models.hdb import HdbBasic
from app.models.condo import CondoBasic
from app.models.agent import Agent
from app.routers.recommendations import _score_candidate

# Ensure environment variables are loaded
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / "backend" / ".env")
except ImportError:
    pass

# Define 3 Synthetic Fake Users based on common Singapore demographics
SYNTHETIC_USERS = {
    "Synthetic User A (East Coast Family Upgrader)": {
        "profile": {
            "property_types": {"Condominium": 5},
            "districts": {15: 5},
            "avg_price": 2000000,
            "avg_beds": 3,
            "buy_rent": "property-for-sale",
            "total_favs": 5,
            "facilities": {"pool": 5, "gym": 5, "tennis": 0, "security": 5, "parking": 5}
        }
    },
    "Synthetic User B (Luxury Landed Buyer)": {
        "profile": {
            "property_types": {"Landed": 5, "Good Class Bungalow": 2},
            "districts": {10: 4, 11: 3},
            "avg_price": 8000000,
            "avg_beds": 5,
            "buy_rent": "property-for-sale",
            "total_favs": 7,
            "facilities": {"pool": 2, "gym": 0, "tennis": 0, "security": 0, "parking": 7}
        }
    },
    "Synthetic User C (Budget Expat Rental)": {
        "profile": {
            "property_types": {"Condominium": 5, "HDB": 2},
            "districts": {2: 3, 3: 2},
            "avg_price": 4500,
            "avg_beds": 2,
            "buy_rent": "property-for-rent",
            "total_favs": 5,
            "facilities": {"pool": 5, "gym": 5, "tennis": 0, "security": 0, "parking": 0}
        }
    }
}

async def run_eval():
    print("\n🚀 Starting Hybrid Recommendation Engine Evaluation (Synthetic Users)\n" + "="*70)
    
    async with AsyncSessionLocal() as session:
        # Load sample listings from the database
        query = select(Listing).options(
            selectinload(Listing.agent),
            selectinload(Listing.condo)
        ).where(Listing.is_active == True).limit(5000)
        
        result = await session.execute(query)
        all_candidates = result.scalars().all()
        print(f"Loaded {len(all_candidates)} active listings from database.\n")

        for user_name, data in SYNTHETIC_USERS.items():
            profile = data["profile"]
            print(f"Testing: {user_name}")
            print(f"Goal: {profile['avg_beds']}BR {profile['buy_rent'].split('-')[-1]} in D{list(profile['districts'].keys())[0]} around ${profile['avg_price']:,}\n")
            
            scored = []
            for c in all_candidates:
                # Pre-filter by mode just like the real API
                if c.buy_rent != profile["buy_rent"]:
                    continue
                    
                score, reasons, val = _score_candidate(c, profile)
                if score > 0:
                    scored.append((score, reasons, c))
            
            # Rank candidates
            scored.sort(key=lambda x: x[0], reverse=True)
            top_5 = scored[:5]
            
            # Print top 5 recommendations
            print(f"{'Rank':<5} | {'Score':<6} | {'Title':<25} | {'Type':<12} | {'Dist':<4} | {'Price':<11} | {'Beds'}")
            print("-" * 80)
            
            for idx, (score, reasons, c) in enumerate(top_5, 1):
                price_str = f"${c.price:,.0f}" if c.price else "N/A"
                dist_str  = str(c.district) if c.district is not None else "-"
                beds_str  = str(c.beds) if c.beds is not None else "-"
                print(f"{idx:<5} | {score:>.3f} | {c.title[:25]:<25} | {str(c.property_type)[:12]:<12} | D{dist_str:<3} | {price_str:<11} | {beds_str} BR")
            
            print("=" * 80 + "\n")

if __name__ == "__main__":
    asyncio.run(run_eval())
