"""Run the AI orchestrator with graceful signal-aware shutdown."""

import uvicorn

from shared.config import AgentSettings
from shared.logging import configure_logging


def main() -> None:
    """Read environment configuration and start the agent health/metrics server."""
    settings = AgentSettings()
    configure_logging()
    uvicorn.run(
        "services.agent.main:app",
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
    )


if __name__ == "__main__":
    main()
