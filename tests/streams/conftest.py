import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "inbound_stream: the case writes a workflow's inbound stream from outside",
    )
    config.addinivalue_line(
        "markers",
        "unfiltered_read: the case reads the owner's stream without naming a topic",
    )
