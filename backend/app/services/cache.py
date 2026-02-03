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
        val = await self.redis.get(key)
        if val:
            return json.loads(val)
        return None

    async def set(self, key: str, value: Any, ttl: int = None):
        if ttl is None:
            ttl = self.default_ttl
        await self.redis.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str):
        await self.redis.delete(key)
        
    async def clear_listings_cache(self):
        # Scan for keys starting with 'listings:' and delete them
        # Note: In production with huge keyspace, SCAN is better but slower. 
        # For this scale, it's acceptable or use better key strategies (e.g. key versioning).
        keys = []
        async for key in self.redis.scan_iter(match="listings:*"):
            keys.append(key)
        if keys:
            await self.redis.delete(*keys)

cache = CacheService()
