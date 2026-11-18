"""Async Redis clients, expiring token-owned locks, and idempotency reservations."""

from __future__ import annotations

from hashlib import sha256
from types import TracebackType
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from redis.asyncio import Redis

from shared.config import RedisSettings

RELEASE_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def create_redis_client(settings: RedisSettings | None = None) -> Redis:
    """Build a lazy UTF-8 client; the caller owns its aclose lifecycle."""
    settings = settings or RedisSettings()
    return Redis(
        host=settings.host,
        port=settings.port,
        db=settings.db,
        socket_connect_timeout=settings.socket_timeout_seconds,
        socket_timeout=settings.socket_timeout_seconds,
        decode_responses=True,
    )


RENEW_IF_OWNER = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""


class LockNotAcquired(RuntimeError):
    """The requested lock is currently held by another owner."""


class RedisLock:
    """Nonblocking SET NX PX lease with a unique token per successful acquisition.

    The holder must finish work within ttl_ms. No implicit lease renewal is performed.
    A stale holder can never delete a newer holder's lock via release().
    """

    def __init__(self, client: Redis, name: str, *, ttl_ms: int = 30_000) -> None:
        """Require a name and positive millisecond expiry before contacting Redis."""
        if not name or isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int) or ttl_ms <= 0:
            raise ValueError("Lock requires a nonempty name and positive integer ttl_ms")
        self.client = client
        self.key = f"lock:{name}"
        self.ttl_ms = ttl_ms
        self._token: str | None = None

    async def acquire(self) -> bool:
        """Try once; return False on contention without modifying the existing lease."""
        if self._token is not None:
            raise RuntimeError("Release this lock instance before acquiring it again")
        token = uuid4().hex
        acquired = await self.client.set(self.key, token, nx=True, px=self.ttl_ms)
        if acquired:
            self._token = token
        return bool(acquired)

    async def release(self) -> bool:
        """Atomically compare token and delete; retain token if the network call fails."""
        if self._token is None:
            return False
        removed = await evaluate(self.client, RELEASE_IF_OWNER, 1, self.key, self._token)
        self._token = None
        return bool(removed)

    async def __aenter__(self) -> RedisLock:
        """Acquire the lease or raise immediately when another owner holds it."""
        if not await self.acquire():
            raise LockNotAcquired(f"Lock {self.key} is already held")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release on normal exit, cancellation, or an exception in the body."""
        await self.release()


class IdempotencyStore:
    """TTL-bound Redis reservations mapping opaque client keys to task UUIDs.

    The database UNIQUE constraint remains the durable source of truth after expiry.
    """

    def __init__(self, client: Redis, *, namespace: str = "task-idempotency") -> None:
        """Keep idempotency keys separate from distributed lock keys."""
        if not namespace:
            raise ValueError("namespace must not be empty")
        self.client = client
        self.namespace = namespace

    def _key(self, key: str) -> str:
        if not key.strip():
            raise ValueError("idempotency key must not be empty")
        return f"{self.namespace}:{sha256(key.encode('utf-8')).hexdigest()}"

    async def reserve(self, key: str, task_id: UUID, *, ttl_seconds: int = 86_400) -> bool:
        """Reserve an unused key; never replace an existing task mapping."""
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive integer")
        return bool(await self.client.set(self._key(key), str(task_id), nx=True, ex=ttl_seconds))

    async def get(self, key: str) -> UUID | None:
        """Return the reserved task UUID or None when absent/expired."""
        value = await self.client.get(self._key(key))
        return UUID(value) if value is not None else None

    async def release(self, key: str, task_id: UUID) -> bool:
        """Undo only the matching reservation, e.g. after its DB transaction fails."""
        return bool(await evaluate(self.client, RELEASE_IF_OWNER, 1, self._key(key), str(task_id)))


class AsyncLuaClient(Protocol):
    """Accurate async boundary for Redis's shared sync/async Lua mixin annotations."""

    async def eval(self, script: str, numkeys: int, *args: str | int | float) -> Any:
        """Execute Lua with Redis-supported numeric or text arguments."""
        ...


async def evaluate(client: Redis, script: str, numkeys: int, *args: str | int | float) -> Any:
    """Redis asyncio always returns an awaitable despite its vendor mixin's union annotation."""
    return await cast(AsyncLuaClient, client).eval(script, numkeys, *args)
