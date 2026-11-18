"""Keep infrastructure tests opt-in for ordinary pytest runs."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Expose an explicit switch for tests that contact local Docker services."""
    parser.addoption("--run-integration", action="store_true", default=False)
    parser.addoption("--run-java", action="store_true", default=False)
    parser.addoption("--run-chaos", action="store_true", default=False)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip integration-marked tests unless requested."""
    if not config.getoption("--run-chaos"):
        for item in items:
            if item.get_closest_marker("chaos") is not None:
                item.add_marker(
                    pytest.mark.skip(reason="use scripts/test_stack.py chaos for isolated faults")
                )
    if not config.getoption("--run-java"):
        for item in items:
            if item.get_closest_marker("java") is not None:
                item.add_marker(
                    pytest.mark.skip(reason="use --run-java with the Java profile/toolchain")
                )
    if not config.getoption("--run-integration"):
        for item in items:
            if item.get_closest_marker("integration") is not None:
                item.add_marker(pytest.mark.skip(reason="use --run-integration with make up"))
