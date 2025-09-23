"""
Cache utilities for UC modules
Redis-based caching with TTL support
"""

import redis
import json
import hashlib
from typing import Any, Optional
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)


class CacheManager:
    """Centralized cache management"""
    
    @staticmethod
    def get_key(*args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_data = {"args": args, "kwargs": kwargs}
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return f"georetail:v2:{hashlib.md5(key_string.encode()).hexdigest()}"
    
    @staticmethod
    def get(key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            value = redis_client.get(key)
            if value:
                return json.loads(value)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None
    
    @staticmethod
    def set(key: str, value: Any, ttl: int = 300):
        """Set value in cache with TTL"""
        try:
            redis_client.setex(
                key,
                timedelta(seconds=ttl),
                json.dumps(value, default=str)
            )
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    @staticmethod
    def delete(key: str):
        """Delete key from cache"""
        try:
            redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
    
    @staticmethod
    def clear_pattern(pattern: str):
        """Clear all keys matching pattern"""
        try:
            for key in redis_client.scan_iter(f"georetail:v2:{pattern}*"):
                redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
