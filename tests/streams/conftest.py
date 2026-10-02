import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")

# The Core the bridge pins decides whether a workflow's subscribe command
# reaches the server. The protos-only pin refuses it; the delivery pin that
# the native layers move to handles it.
PINNED_CORE_HANDLES_CHANNEL_COMMAND = True


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
        "needs_channel_server: the case needs a server that serves notification "
        "channels, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_channel_core: the case needs the pinned Core to handle the "
        "subscribe-notification-channel command",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    # The dev server the suite starts for itself does not accept the
    # subscribe command, so the live channel cases run only against a server
    # the caller points at.
    skips: list[tuple[str, pytest.MarkDecorator]] = []
    if config.getoption("--workflow-environment") in _ENVIRONMENTS_WITHOUT_CHANNELS:
        skips.append(
            (
                "needs_channel_server",
                pytest.mark.skip(
                    reason="needs a server that serves notification channels; "
                    "name one with -E"
                ),
            )
        )
    if not PINNED_CORE_HANDLES_CHANNEL_COMMAND:
        skips.append(
            (
                "needs_channel_core",
                pytest.mark.skip(
                    reason="the pinned Core refuses the subscribe command; py-05 "
                    "pins one that handles it"
                ),
            )
        )
    for item in items:
        for marker, skip in skips:
            if item.get_closest_marker(marker):
                item.add_marker(skip)
