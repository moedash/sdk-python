import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")


def pytest_configure(config: pytest.Config) -> None:
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
    for item in items:
        for marker, skip in skips:
            if item.get_closest_marker(marker):
                item.add_marker(skip)
