"""Run the dedicated DLQ consumer and its health endpoint."""

import uvicorn

from shared.config import DLQSettings
from shared.logging import configure_logging


def main() -> None:
    """Route process signals through the consumer's graceful shutdown."""
    settings = DLQSettings()
    configure_logging()
    uvicorn.run(
        "services.dlq.main:app",
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
