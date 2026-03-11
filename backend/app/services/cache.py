import redis.asyncio as redis
import json
from typing import Optional, Any
from app.config import settings

class CacheService:
    def __init__(self):
        self.redis = redis.from_url(
            f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}",
            encoding="utf-8",
            decode_responses=True
        )
        self.default_ttl = 300  # 5 minutes

    async def get(self, key: str) -> Optional[Any]:
        try:
            val = await self.redis.get(key)
            if val:
                return json.loads(val)
        except Exception as e:
            print(f"Redis cache GET error: {e}")
        return None

    async def set(self, key: str, value: Any, ttl: int = None):
        if ttl is None:
            ttl = self.default_ttl
        try:
            await self.redis.set(key, json.dumps(value), ex=ttl)
        except Exception as e:
            print(f"Redis cache SET error: {e}")

    async def delete(self, key: str):
        try:
            await self.redis.delete(key)
        except Exception as e:
            print(f"Redis cache DELETE error: {e}")
        
    async def clear_listings_cache(self):
        try:
            keys = []
            async for key in self.redis.scan_iter(match="listings:*"):
                keys.append(key)
            if keys:
                await self.redis.delete(*keys)
        except Exception as e:
            print(f"Redis cache CLEAR error: {e}")

cache = CacheService()
