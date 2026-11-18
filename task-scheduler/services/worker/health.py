"""Container health probe backed by a live, token-matched Redis worker heartbeat."""

import asyncio
import json

from shared.config import WorkerSettings
from shared.redis import create_redis_client


async def check() -> bool:
    """Require the local worker identity to still own a nonexpired heartbeat."""
    settings = WorkerSettings()
    client = create_redis_client()
    try:
        value = settings.health_file.read_text(encoding="utf-8")
        worker_id = json.loads(value)["worker_id"]
        return await client.get(f"worker:{worker_id}") == value
    except (OSError, ValueError, KeyError):
        return False
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(0 if asyncio.run(check()) else 1)
