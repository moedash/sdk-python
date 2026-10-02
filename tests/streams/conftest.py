import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")


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
    config.addinivalue_line(
        "markers",
        "needs_channel_server: the case needs a server that serves notification "
        "channels, named with -E host:port",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    # The dev server the suite starts for itself does not accept the
    # subscribe command, so the live channel cases run only against a server
    # the caller points at.
    if config.getoption("--workflow-environment") not in _ENVIRONMENTS_WITHOUT_CHANNELS:
        return
    skip = pytest.mark.skip(
        reason="needs a server that serves notification channels; name one with -E"
    )
    for item in items:
        if item.get_closest_marker("needs_channel_server"):
            item.add_marker(skip)
