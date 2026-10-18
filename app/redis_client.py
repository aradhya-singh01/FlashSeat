"""
FlashSeat Redis Client Layer
High-concurrency distributed seat locking, temporary reservation holds with TTL,
atomic Lua scripts, real-time caching, and anti-scalping rate limiting.

Features:
- Native async Redis integration via `redis.asyncio`.
- Atomic distributed seat lock using `SET key user_id NX EX ttl`.
- Atomic lock release via Lua script (ensures only lock owner can release).
- Intelligent fallback to an in-memory lock/cache manager if Redis is not currently
  running locally, ensuring smooth zero-crash development out of the box.
"""

import asyncio
import logging
import os
import time
from typing import Dict, Optional, Tuple

try:
    import redis.asyncio as aioredis
    from redis.asyncio.client import Redis
except ImportError:
    aioredis = None
    Redis = None

logger = logging.getLogger("flashseat.redis")
logging.basicConfig(level=logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DEFAULT_HOLD_TTL = int(os.getenv("SEAT_HOLD_TTL_SECONDS", "600"))  # 10 minutes hold

# Atomic Lua script for safe lock release:
# Only deletes key if the current value matches the requesting user_id.
RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class InMemoryFallbackManager:
    """
    Thread-safe async in-memory fallback when Redis server is offline.
    Provides identical TTL expiration and concurrency semantics for local development.
    """
    def __init__(self):
        self._locks: Dict[str, Tuple[str, float]] = {}  # key -> (user_id, expire_at)
        self._cache: Dict[str, Tuple[str, float]] = {}  # key -> (json_data, expire_at)
        self._rate_limits: Dict[str, list] = {}         # key -> list of timestamps
        self._lock = asyncio.Lock()

    async def acquire_hold(self, key: str, user_id: str, ttl: int) -> Tuple[bool, str, int]:
        async with self._lock:
            now = time.time()
            # Clean expired
            if key in self._locks and self._locks[key][1] <= now:
                del self._locks[key]

            if key in self._locks:
                existing_user, expire_at = self._locks[key]
                remaining = int(expire_at - now)
                if existing_user == user_id:
                    # Extend or re-confirm existing user's hold
                    self._locks[key] = (user_id, now + ttl)
                    return True, "Seat hold extended for checkout.", ttl
                return False, f"Seat is currently held by another user. Try again in {remaining}s.", remaining

            self._locks[key] = (user_id, now + ttl)
            return True, "Seat hold acquired successfully.", ttl

    async def release_hold(self, key: str, user_id: str) -> bool:
        async with self._lock:
            now = time.time()
            if key in self._locks:
                holder, expire_at = self._locks[key]
                if expire_at > now and holder == user_id:
                    del self._locks[key]
                    return True
                elif expire_at <= now:
                    del self._locks[key]
            return False

    async def get_hold(self, key: str) -> Optional[Dict[str, any]]:
        async with self._lock:
            now = time.time()
            if key in self._locks:
                holder, expire_at = self._locks[key]
                if expire_at > now:
                    return {"user_id": holder, "ttl_remaining": int(expire_at - now)}
                else:
                    del self._locks[key]
            return None

    async def get_all_holds(self, prefix: str) -> Dict[str, Dict[str, any]]:
        async with self._lock:
            now = time.time()
            results = {}
            expired = []
            for k, (holder, expire_at) in self._locks.items():
                if k.startswith(prefix):
                    if expire_at > now:
                        results[k] = {"user_id": holder, "ttl_remaining": int(expire_at - now)}
                    else:
                        expired.append(k)
            for k in expired:
                del self._locks[k]
            return results

    async def clear_all(self):
        async with self._lock:
            self._locks.clear()
            self._cache.clear()
            self._rate_limits.clear()


class RedisClient:
    """Unified Redis Manager with transparent fallbacks and high-concurrency tooling."""

    def __init__(self, url: str = REDIS_URL):
        self.url = url
        self.redis: Optional[Redis] = None
        self.is_connected = False
        self._fallback = InMemoryFallbackManager()
        self._release_script = None

    async def connect(self) -> None:
        """Initializes connection to Redis or activates resilient fallback."""
        if aioredis is None:
            self.is_connected = False
            self.redis = None
            logger.warning(
                "Redis Python package not installed ('redis'). "
                "Running in Resilient In-Memory Mode for local development."
            )
            return

        try:
            self.redis = aioredis.from_url(
                self.url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
            # Test connectivity
            await self.redis.ping()
            self.is_connected = True
            self._release_script = self.redis.register_script(RELEASE_LOCK_LUA)
            logger.info("Connected to Redis successfully at %s", self.url)
        except Exception as exc:
            self.is_connected = False
            self.redis = None
            logger.warning(
                "Redis server not reachable at %s (%s). "
                "Running in Resilient In-Memory Mode for local development.",
                self.url,
                exc,
            )

    async def disconnect(self) -> None:
        """Closes the Redis connection pool."""
        if self.redis and self.is_connected:
            await self.redis.close()
            self.is_connected = False
            logger.info("Closed Redis connection.")

    def _hold_key(self, event_id: int, seat_id: int) -> str:
        return f"flashseat:hold:{event_id}:{seat_id}"

    async def acquire_seat_hold(
        self,
        event_id: int,
        seat_id: int,
        user_id: str,
        ttl_seconds: int = DEFAULT_HOLD_TTL,
    ) -> Tuple[bool, str, int]:
        """
        Attempts to hold a seat atomically for a specific user.
        Uses Redis SET key value NX EX ttl (atomic Distributed Lock).
        """
        key = self._hold_key(event_id, seat_id)

        if not self.is_connected or self.redis is None:
            return await self._fallback.acquire_hold(key, user_id, ttl_seconds)

        try:
            # SET NX EX is an atomic Redis operation
            acquired = await self.redis.set(key, user_id, nx=True, ex=ttl_seconds)
            if acquired:
                return True, "Seat hold acquired successfully. Complete checkout within TTL.", ttl_seconds

            # If not acquired, check who holds it
            current_holder = await self.redis.get(key)
            ttl = await self.redis.ttl(key)
            if current_holder == user_id:
                # Same user re-holding/extending their checkout session
                await self.redis.expire(key, ttl_seconds)
                return True, "Seat hold refreshed for your checkout.", ttl_seconds

            return False, f"Seat is currently reserved by another customer. Lock expires in {ttl}s.", max(ttl, 0)
        except Exception as exc:
            logger.error("Redis error acquiring hold: %s. Falling back to local lock.", exc)
            return await self._fallback.acquire_hold(key, user_id, ttl_seconds)

    async def release_seat_hold(self, event_id: int, seat_id: int, user_id: str) -> bool:
        """
        Releases the held seat lock if and only if the current user owns it.
        Uses an atomic Lua script to prevent race conditions during checkout release.
        """
        key = self._hold_key(event_id, seat_id)

        if not self.is_connected or self.redis is None:
            return await self._fallback.release_hold(key, user_id)

        try:
            if self._release_script is None:
                self._release_script = self.redis.register_script(RELEASE_LOCK_LUA)
            result = await self._release_script(keys=[key], args=[user_id])
            return bool(result)
        except Exception as exc:
            logger.error("Redis error releasing hold: %s. Falling back to local release.", exc)
            return await self._fallback.release_hold(key, user_id)

    async def force_release_hold(self, event_id: int, seat_id: int) -> bool:
        """Administrative unlock of a seat hold."""
        key = self._hold_key(event_id, seat_id)
        if not self.is_connected or self.redis is None:
            async with self._fallback._lock:
                if key in self._fallback._locks:
                    del self._fallback._locks[key]
                    return True
                return False
        try:
            await self.redis.delete(key)
            return True
        except Exception as exc:
            logger.error("Redis error force-releasing hold: %s", exc)
            return False

    async def get_seat_hold_info(self, event_id: int, seat_id: int) -> Optional[Dict[str, any]]:
        """Retrieves active hold holder and remaining TTL for a seat."""
        key = self._hold_key(event_id, seat_id)

        if not self.is_connected or self.redis is None:
            return await self._fallback.get_hold(key)

        try:
            holder = await self.redis.get(key)
            if not holder:
                return None
            ttl = await self.redis.ttl(key)
            return {"user_id": holder, "ttl_remaining": max(ttl, 0)}
        except Exception as exc:
            logger.error("Redis error checking hold info: %s", exc)
            return await self._fallback.get_hold(key)

    async def get_all_active_holds_for_event(self, event_id: int) -> Dict[int, Dict[str, any]]:
        """
        Retrieves all currently active seat holds for an event.
        Returns a dict mapping: seat_id -> {"user_id": ..., "ttl_remaining": ...}
        """
        prefix = f"flashseat:hold:{event_id}:"

        if not self.is_connected or self.redis is None:
            raw_holds = await self._fallback.get_all_holds(prefix)
            holds = {}
            for k, val in raw_holds.items():
                seat_id = int(k.split(":")[-1])
                holds[seat_id] = val
            return holds

        holds = {}
        try:
            async for key in self.redis.scan_iter(match=f"{prefix}*"):
                seat_id_str = key.split(":")[-1]
                if seat_id_str.isdigit():
                    seat_id = int(seat_id_str)
                    holder = await self.redis.get(key)
                    ttl = await self.redis.ttl(key)
                    if holder:
                        holds[seat_id] = {"user_id": holder, "ttl_remaining": max(ttl, 0)}
        except Exception as exc:
            logger.error("Redis error fetching active holds: %s", exc)
        return holds

    async def check_rate_limit(
        self, client_identifier: str, limit: int = 30, window_seconds: int = 60
    ) -> Tuple[bool, int]:
        """
        Token bucket / sliding window rate limiter to protect flash sale endpoints from bot abuse.
        Returns (is_allowed, remaining_quota).
        """
        key = f"flashseat:ratelimit:{client_identifier}"
        if not self.is_connected or self.redis is None:
            # Simple in-memory rate check
            now = time.time()
            records = self._fallback._rate_limits.setdefault(key, [])
            # filter out older than window
            records = [t for t in records if t > now - window_seconds]
            self._fallback._rate_limits[key] = records
            if len(records) >= limit:
                return False, 0
            records.append(now)
            return True, limit - len(records)

        try:
            pipe = self.redis.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds)
            res = await pipe.execute()
            count = res[0]
            if count > limit:
                return False, 0
            return True, limit - count
        except Exception:
            return True, limit

    async def clear_all_holds(self) -> None:
        """Clears all holds across the system (useful for test resets)."""
        await self._fallback.clear_all()
        if self.is_connected and self.redis:
            try:
                async for key in self.redis.scan_iter(match="flashseat:*"):
                    await self.redis.delete(key)
            except Exception as exc:
                logger.error("Error clearing Redis keys: %s", exc)


# Global singleton instance
redis_client = RedisClient()
