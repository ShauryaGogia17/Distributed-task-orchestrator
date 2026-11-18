"""Signal-aware worker entrypoint suitable for Docker replicas or a host process."""

import asyncio
import signal

from services.worker.service import WorkerService
from shared.config import get_settings
from shared.db import create_engine, create_session_factory
from shared.logging import configure_logging
from shared.redis import create_redis_client


async def serve() -> None:
    """Own engine/Redis cleanup and route termination signals to graceful draining."""
    settings = get_settings()
    engine = create_engine(settings.postgres)
    client = create_redis_client(settings.redis)
    service = WorkerService(create_session_factory(engine), client, settings)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, service.request_stop)
    try:
        await service.run()
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(signum)
        await client.aclose()
        await engine.dispose()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(serve())
