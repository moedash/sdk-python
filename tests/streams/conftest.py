import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "reports_positions: the case needs append() to return where records landed",
    )
