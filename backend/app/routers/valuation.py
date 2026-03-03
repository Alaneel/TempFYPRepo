from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from app.services import valuation as valuation_service

router = APIRouter()


class EstimateRequest(BaseModel):
    property_type: str = Field(..., examples=["Condominium"])
    buy_rent:      str = Field(..., examples=["property-for-sale"])
    beds:          float = Field(..., ge=0, le=20)
    sqft:          float = Field(..., ge=50, le=50_000)
    tenure:        Optional[str]   = Field(None, examples=["Freehold"])
    actual_price:  Optional[float] = Field(None, ge=0)
    built_year:    Optional[int]   = Field(None, ge=1960, le=2035, description="Year of TOP/completion")
    postal_code:   Optional[str]   = Field(None, examples=["238859"], description="6-digit Singapore postal code")
    district:      Optional[int]   = Field(None, ge=1, le=28, description="Singapore district number (1-28)")


@router.post("/estimate")
def estimate(req: EstimateRequest):
    """
    Returns a property price estimate with confidence range, over/under-priced
    verdict (if actual_price provided), and SHAP-based attribution factors.
    """
    try:
        result = valuation_service.estimate(
            property_type=req.property_type,
            buy_rent=req.buy_rent,
            beds=req.beds,
            sqft=req.sqft,
            tenure=req.tenure,
            actual_price=req.actual_price,
            built_year=req.built_year,
            postal_code=req.postal_code,
            district=req.district,
        )
        return result
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Valuation error: {e}")


@router.get("/health")
def health():
    """Check model availability."""
    return valuation_service.health()
