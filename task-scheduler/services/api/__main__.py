"""Run the HTTP API with environment settings and structured service logs."""

import uvicorn

from shared.config import APISettings
from shared.logging import configure_logging


def main() -> None:
    """Start one process; each replica owns an independent SKIP LOCKED publisher."""
    settings = APISettings()
    configure_logging()
    uvicorn.run(
        "services.api.main:app",
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
