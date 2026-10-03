import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "needs_channel_server: the case needs a server that serves notification "
        "channels, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_server: the case needs a server that serves channels linked "
        "to a workflow, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_describe_server: the case needs a server whose workflow description "
        "lists the channel subscriptions, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_unsubscribe_server: the case needs a server that accepts the "
        "unsubscribe-notification-channel command, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_execution_server: the case needs a server that addresses a linked "
        "channel by execution, a standalone activity's included, named with "
        "-E host:port",
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
    if config.getoption("--workflow-environment") in _ENVIRONMENTS_WITHOUT_CHANNELS:
        skips.append(
            (
                "needs_linked_server",
                pytest.mark.skip(
                    reason="needs a server with channels linked to a workflow; "
                    "name one with -E"
                ),
            )
        )
    if config.getoption("--workflow-environment") in _ENVIRONMENTS_WITHOUT_CHANNELS:
        for marker, what in (
            ("needs_describe_server", "lists channel subscriptions on describe"),
            ("needs_unsubscribe_server", "accepts the unsubscribe command"),
            ("needs_execution_server", "addresses a linked channel by execution"),
        ):
            skips.append(
                (
                    marker,
                    pytest.mark.skip(
                        reason=f"needs a server that {what}; name one with -E"
                    ),
                )
            )
    for item in items:
        for marker, skip in skips:
            if item.get_closest_marker(marker):
                item.add_marker(skip)
