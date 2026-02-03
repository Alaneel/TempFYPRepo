from pydantic import BaseModel
from typing import Optional, List, Generic, TypeVar

T = TypeVar('T')

class PaginationParams(BaseModel):
    page: int = 1
    limit: int = 20

class PaginatedResponse(BaseModel, Generic[T]):
    total: int
    page: int
    limit: int
    data: List[T]

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenPayload(BaseModel):
    sub: Optional[str] = None
    role: Optional[str] = None
