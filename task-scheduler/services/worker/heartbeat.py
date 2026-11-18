"""Token-owned Redis worker heartbeat and container readiness identity."""

from __future__ import annotations

import asyncio
import json
from uuid import uuid4

from redis.asyncio import Redis

from shared.config import WorkerSettings
from shared.redis import RELEASE_IF_OWNER, RENEW_IF_OWNER, evaluate


class WorkerHeartbeat:
    """Reserve worker:{id}, refresh its TTL, and remove only this process's value."""

    def __init__(self, client: Redis, worker_id: str, settings: WorkerSettings) -> None:
        self.client = client
        self.worker_id = worker_id
        self.settings = settings
        self.key = f"worker:{worker_id}"
        self.value = json.dumps({"worker_id": worker_id, "token": uuid4().hex})

    async def start(self) -> None:
        """Reject duplicate live worker IDs and write the local readiness identity."""
        if not await self.client.set(
            self.key, self.value, nx=True, ex=self.settings.heartbeat_ttl_seconds
        ):
            raise RuntimeError(f"Worker ID already active: {self.worker_id}")
        self.settings.health_file.write_text(self.value, encoding="utf-8")

    async def run(self) -> None:
        """Fail visibly if Redis is unavailable or another owner replaces this key."""
        while True:
            await asyncio.sleep(self.settings.heartbeat_interval_seconds)
            renewed = await evaluate(
                self.client,
                RENEW_IF_OWNER,
                1,
                self.key,
                self.value,
                self.settings.heartbeat_ttl_seconds * 1000,
            )
            if not renewed:
                raise RuntimeError("Worker heartbeat ownership was lost")

    async def close(self) -> None:
        """Release only the owned heartbeat and matching local identity file."""
        await evaluate(self.client, RELEASE_IF_OWNER, 1, self.key, self.value)
        if (
            self.settings.health_file.exists()
            and self.settings.health_file.read_text(encoding="utf-8") == self.value
        ):
            self.settings.health_file.unlink()
