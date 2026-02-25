from pydantic_settings import BaseSettings
from typing import Optional
import os

# Resolve .env from project root (two levels up from this file: backend/app/config.py)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_ENV_FILE = os.path.join(_PROJECT_ROOT, ".env")

class Settings(BaseSettings):
    PROJECT_NAME: str = "Real Estate API"
    API_V1_STR: str = "/api/v1"
    
    # Database
    DB_HOST: str = "localhost"
    DB_PORT: str = "5432"
    DB_USER: str = "alanwang"
    DB_PASS: str = ""
    DB_NAME: str = "real_estate_app"
    
    # JWT
    SECRET_KEY: str = "YOUR_SECRET_KEY_HERE_CHANGE_IN_PRODUCTION"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # AI
    ANTHROPIC_API_KEY: Optional[str] = None
    
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    class Config:
        env_file = _ENV_FILE
        case_sensitive = True

settings = Settings()
