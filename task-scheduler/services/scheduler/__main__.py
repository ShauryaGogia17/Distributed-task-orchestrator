"""Run the scheduler with independently scrapeable metrics and signal-aware lifespan."""

import uvicorn

from shared.config import SchedulerSettings
from shared.logging import configure_logging


def main() -> None:
    """Uvicorn routes SIGTERM through graceful leader release and outbox shutdown."""
    settings = SchedulerSettings()
    configure_logging()
    uvicorn.run(
        "services.scheduler.main:app",
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
