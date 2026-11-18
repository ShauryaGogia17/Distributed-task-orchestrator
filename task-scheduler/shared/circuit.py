"""Redis task-type circuits with atomic single probes and fenced completion/renewal."""

from dataclasses import dataclass
from hashlib import sha256
from uuid import uuid4

from redis.asyncio import Redis

from shared.config import CircuitSettings
from shared.redis import evaluate

OPEN = """
if redis.call('HGET', KEYS[1], 'epoch') == ARGV[1] then return 0 end
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local target = ms + tonumber(ARGV[2])
if tonumber(redis.call('HGET', KEYS[1], 'until') or '0') >= target then return 0 end
redis.call('HSET', KEYS[1], 'state', 'OPEN', 'epoch', ARGV[1],
 'until', target, 'probe', '', 'probe_until', 0)
return 1
"""
ACQUIRE = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {1, '', '', 0} end
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local until_ms = tonumber(redis.call('HGET', KEYS[1], 'until') or '0')
local probe_until = tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0')
if ms < until_ms then return {0, '', '', until_ms - ms} end
if ms < probe_until then return {0, '', '', probe_until - ms} end
local epoch = redis.call('HGET', KEYS[1], 'epoch')
redis.call('HSET', KEYS[1], 'state', 'HALF_OPEN', 'probe', ARGV[1],
 'probe_until', ms + tonumber(ARGV[2]))
return {1, epoch, ARGV[1], 0}
"""
RENEW = """
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1]
 or redis.call('HGET', KEYS[1], 'probe') ~= ARGV[2]
 or tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0') <= ms then return 0 end
redis.call('HSET', KEYS[1], 'probe_until', ms + tonumber(ARGV[3]))
return 1
"""
FINISH = """
local now = redis.call('TIME')
local ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1]
 or redis.call('HGET', KEYS[1], 'probe') ~= ARGV[2]
 or tonumber(redis.call('HGET', KEYS[1], 'probe_until') or '0') <= ms then return 0 end
if ARGV[3] == '1' then return redis.call('DEL', KEYS[1]) end
redis.call('HSET', KEYS[1], 'state', 'OPEN', 'until', ms + tonumber(ARGV[4]),
 'probe', '', 'probe_until', 0)
return 1
"""


@dataclass(frozen=True)
class Permit:
    """Closed traffic has no probe token; half-open completion is fenced by both IDs."""

    allowed: bool
    epoch: str = ""
    token: str = ""
    retry_after_s: float = 0


class CircuitBreaker:
    """One persistent circuit per task type, shared by all worker replicas."""

    def __init__(self, redis: Redis, settings: CircuitSettings) -> None:
        self.redis, self.settings = redis, settings

    def key(self, task_type: str) -> str:
        """Bound Redis key size and allow independently namespaced integration tests."""
        return self.settings.key_prefix + sha256(task_type.encode()).hexdigest()

    async def open(
        self, task_type: str, *, epoch: str | None = None, seconds: float | None = None
    ) -> None:
        """Idempotent same-epoch restoration never resets a half-open probe or cooldown."""
        await evaluate(
            self.redis,
            OPEN,
            1,
            self.key(task_type),
            epoch or uuid4().hex,
            max(1, int((self.settings.cooldown_seconds if seconds is None else seconds) * 1000)),
        )

    async def acquire(self, task_type: str, lease_ms: int) -> Permit:
        """Permit normal traffic or exactly one renewable probe after cooldown."""
        allowed, epoch, token, delay = await evaluate(
            self.redis, ACQUIRE, 1, self.key(task_type), uuid4().hex, lease_ms
        )
        return Permit(
            bool(allowed), epoch, token, max(0.1, float(delay) / 1000) if not allowed else 0
        )

    async def renew(self, task_type: str, permit: Permit, lease_ms: int) -> bool:
        """An old probe cannot renew or close a later probe's circuit."""
        if not permit.token:
            return True
        return bool(
            await evaluate(
                self.redis, RENEW, 1, self.key(task_type), permit.epoch, permit.token, lease_ms
            )
        )

    async def complete(self, task_type: str, permit: Permit, success: bool) -> None:
        """Close only a successful owned probe; a failed probe starts a new cooldown."""
        if permit.token:
            await evaluate(
                self.redis,
                FINISH,
                1,
                self.key(task_type),
                permit.epoch,
                permit.token,
                int(success),
                int(self.settings.cooldown_seconds * 1000),
            )
