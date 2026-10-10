import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "reads: the case needs the provider to read a stream"
    )
    config.addinivalue_line(
        "markers",
        "live_gaps: the case needs a read in progress to notice dropped records",
    )
