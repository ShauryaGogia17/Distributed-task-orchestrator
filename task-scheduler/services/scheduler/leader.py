"""Expiring, token-fenced Redis leadership with safe renewal and release."""

import json
from uuid import uuid4

from redis.asyncio import Redis

from shared.config import SchedulerSettings
from shared.redis import RELEASE_IF_OWNER, RENEW_IF_OWNER, evaluate


class LeadershipLost(RuntimeError):
    """The current process may no longer perform leader-only work."""


class LeaderLease:
    """One random-token lease per acquisition; Redis expiration enables standby takeover."""

    def __init__(self, client: Redis, instance_id: str, settings: SchedulerSettings) -> None:
        self.client = client
        self.instance_id = instance_id
        self.settings = settings
        self.value = ""
        self.held = False

    async def acquire(self) -> bool:
        """Try once without overwriting another scheduler's lease."""
        if self.held:
            return await self.is_owner()
        value = json.dumps({"instance_id": self.instance_id, "token": uuid4().hex})
        if await self.client.set(
            self.settings.leader_key,
            value,
            nx=True,
            px=int(self.settings.leader_ttl_seconds * 1000),
        ):
            self.value = value
            self.held = True
        return self.held

    async def is_owner(self) -> bool:
        """Verify ownership at the source instead of trusting cached leader state."""
        self.held = bool(
            self.held and await self.client.get(self.settings.leader_key) == self.value
        )
        return self.held

    async def require(self) -> None:
        """Fence DB materialization and release if the lease expired or was replaced."""
        if not await self.is_owner():
            raise LeadershipLost("Scheduler leader lease is no longer owned")

    async def renew(self) -> bool:
        """Extend only this acquisition token, never a successor's lease."""
        self.held = bool(
            await evaluate(
                self.client,
                RENEW_IF_OWNER,
                1,
                self.settings.leader_key,
                self.value,
                int(self.settings.leader_ttl_seconds * 1000),
            )
        )
        return self.held

    async def release(self) -> None:
        """Leave leadership immediately on graceful exit, without deleting a new owner."""
        if self.value:
            await evaluate(self.client, RELEASE_IF_OWNER, 1, self.settings.leader_key, self.value)
        self.held = False
