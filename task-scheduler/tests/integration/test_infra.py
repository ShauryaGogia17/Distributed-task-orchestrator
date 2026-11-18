"""Read-only smoke test against the actual Docker services."""

import pytest

from scripts.check_infra import check_infrastructure
from shared.config import get_settings


@pytest.mark.integration
async def test_live_infrastructure_and_topics() -> None:
    """Authenticate to all services and assert the complete Kafka topic contract."""
    results = await check_infrastructure(get_settings())
    assert len(results) == 3
    assert all(result.ok for result in results), results
