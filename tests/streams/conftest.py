import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "reports_positions: the case needs append() to return where records landed",
    )
    config.addinivalue_line(
        "markers",
        "detects_divergent_retries: the case needs append() to compare a repeat's "
        "content with what the store holds",
    )
    config.addinivalue_line(
        "markers",
        "truncates: the case needs a way to drop a topic's oldest records",
    )
    config.addinivalue_line(
        "markers",
        "encodes_bodies: the case needs the outside path to run each body through "
        "the client's data converter",
    )
    config.addinivalue_line(
        "markers",
        "standalone_activities: the case needs the streams of an activity outside "
        "any workflow",
    )
    config.addinivalue_line(
        "markers",
        "hosts_standalone_streams: the case needs a stream with an id of its own and "
        "no owner",
    )
