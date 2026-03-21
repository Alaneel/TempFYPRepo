import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent / "backend"))

try:
    from app.services import valuation as valuation_service
    import pandas as pd
    import numpy as np

    print("--- Testing Valuation Service ---")
    
    # Test cases: Condo Sale (which exists)
    try:
        print("\n1. Testing Condo Sale (exists):")
        res = valuation_service.estimate(
            property_type="Condominium",
            buy_rent="property-for-sale",
            beds=3,
            sqft=1100,
            tenure="Freehold",
            built_year=2015,
            district=15
        )
        print(f"SUCCESS: {res['estimate']}")
    except Exception as e:
        import traceback
        print(f"FAILED: {e}")
        traceback.print_exc()

    # Test cases: HDB Sale (doesn't exist)
    try:
        print("\n2. Testing HDB Sale (missing model):")
        res = valuation_service.estimate(
            property_type="HDB",
            buy_rent="property-for-sale",
            beds=4,
            sqft=1000,
            tenure="99-year Leasehold",
            built_year=1995,
            district=12
        )
        print(f"SUCCESS: {res['estimate']}")
    except FileNotFoundError as e:
        print(f"CAUGHT EXPECTED ERROR: {e}")
    except Exception as e:
        import traceback
        print(f"FAILED: {e}")
        traceback.print_exc()

except Exception as e:
    import traceback
    print(f"GENERAL ERROR: {e}")
    traceback.print_exc()
