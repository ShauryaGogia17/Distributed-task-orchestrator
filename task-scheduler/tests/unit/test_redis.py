"""Token ownership and error handling for Redis coordination helpers."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from shared.redis import RELEASE_IF_OWNER, IdempotencyStore, LockNotAcquired, RedisLock


async def test_acquisition_uses_nx_px_and_safe_token_release() -> None:
    """Acquire atomically with expiry, then release only the exact owner token."""
    client = AsyncMock()
    client.set.return_value = True
    client.eval.return_value = 1
    lock = RedisLock(client, "scheduler", ttl_ms=1234)
    assert await lock.acquire()
    token = client.set.call_args.args[1]
    assert len(token) == 32
    client.set.assert_awaited_once_with("lock:scheduler", token, nx=True, px=1234)
    with pytest.raises(RuntimeError):
        await lock.acquire()
    assert await lock.release()
    client.eval.assert_awaited_once_with(RELEASE_IF_OWNER, 1, "lock:scheduler", token)
    assert not await lock.release()


async def test_contending_client_cannot_release_another_owner() -> None:
    """A failed acquisition must never issue a delete or a release script."""
    client = AsyncMock()
    client.set.return_value = None
    lock = RedisLock(client, "scheduler")
    assert not await lock.acquire()
    assert not await lock.release()
    with pytest.raises(LockNotAcquired):
        async with lock:
            pytest.fail("a contended lock must not enter its body")
    client.eval.assert_not_awaited()


async def test_context_releases_after_body_exception() -> None:
    """Exceptions must unwind the lock context and still reach its caller."""
    client = AsyncMock()
    client.set.return_value = True
    client.eval.return_value = 1
    with pytest.raises(ValueError, match="body"):
        async with RedisLock(client, "scheduler"):
            raise ValueError("body")
    client.eval.assert_awaited_once()


async def test_release_network_error_retains_token_for_retry() -> None:
    """A failed network call must not discard ownership needed for safe retry."""
    client = AsyncMock()
    client.set.return_value = True
    client.eval.side_effect = [ConnectionError("offline"), 1]
    lock = RedisLock(client, "scheduler")
    await lock.acquire()
    with pytest.raises(ConnectionError):
        await lock.release()
    assert await lock.release()
    assert client.eval.call_args_list[0] == client.eval.call_args_list[1]


@pytest.mark.parametrize("ttl_ms", [0, -1, True, 1.5])
def test_invalid_lock_expiry(ttl_ms: int) -> None:
    """Every lease must have a positive integer expiry."""
    with pytest.raises(ValueError):
        RedisLock(AsyncMock(), "scheduler", ttl_ms=ttl_ms)


async def test_idempotency_reservation_is_namespaced_and_conditional() -> None:
    """Never overwrite an existing reservation and release only the matching task."""
    client = AsyncMock()
    client.set.return_value = True
    task_id = uuid4()
    store = IdempotencyStore(client)
    assert await store.reserve("client-order-1", task_id, ttl_seconds=60)
    key = client.set.call_args.args[0]
    assert key.startswith("task-idempotency:")
    assert "client-order-1" not in key
    client.set.assert_awaited_once_with(key, str(task_id), nx=True, ex=60)
    client.get.return_value = str(task_id)
    assert await store.get("client-order-1") == task_id
    client.get.return_value = None
    assert await store.get("client-order-1") is None
    client.eval.return_value = 0
    assert not await store.release("client-order-1", uuid4())


@pytest.mark.parametrize("ttl", [0, -1, True])
async def test_idempotency_requires_expiry(ttl: int) -> None:
    """Reject reservations that could remain indefinitely."""
    with pytest.raises(ValueError):
        await IdempotencyStore(AsyncMock()).reserve("key", uuid4(), ttl_seconds=ttl)
