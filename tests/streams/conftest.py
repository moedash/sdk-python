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
        "hosts_standalone_streams: the case needs a stream with an id of its own and "
        "no owner",
    )
    config.addinivalue_line(
        "markers",
        "wakes_by_notification: the case needs an outside append to wake a parked "
        "workflow reader through the server",
    )
    config.addinivalue_line(
        "markers",
        "wakes_by_linked_notification: the case needs an outside append to wake a "
        "parked workflow reader through the channel linked to its workflow",
    )
    config.addinivalue_line(
        "markers",
        "needs_channel_server: the case needs a server that serves notification "
        "channels, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_server: the case needs a server that serves channels linked "
        "to a workflow, named with -E host:port; the case skips itself on one "
        "with only independent channels",
    )


#: The environments whose server the suite starts for itself. None of them
#: accepts the subscribe-notification-channel command.
_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--workflow-environment") not in _ENVIRONMENTS_WITHOUT_CHANNELS:
        return
    skip = pytest.mark.skip(
        reason="needs a server that serves notification channels; name one with -E"
    )
    for item in items:
        if item.get_closest_marker("needs_channel_server") or item.get_closest_marker(
            "needs_linked_server"
        ):
            item.add_marker(skip)
