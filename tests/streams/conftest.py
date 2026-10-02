import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")

# The Core the bridge pins decides whether a workflow's subscribe command
# reaches the server. The protos-only pin refuses it; the delivery pin that
# the native layers move to handles it.
PINNED_CORE_HANDLES_CHANNEL_COMMAND = True

# A channel linked to a workflow needs no command, but a notification reaches
# workflow code as the `NotificationsReceived` job Core builds from the
# scheduled event, and the protos-only pin ignores that job. So a linked case
# in which the workflow receives waits for the delivery pin as well; one that
# only talks to the server from the client runs on every layer.
PINNED_CORE_HANDLES_LINKED_CHANNEL = True


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
        "needs_channel_core: the case needs the pinned Core to handle the "
        "subscribe-notification-channel command",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_server: the case needs a server that serves channels linked "
        "to a workflow, named with -E host:port; the case skips itself on one "
        "with only independent channels",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_core: the case needs the pinned Core to hand a linked "
        "channel's notifications to workflow code",
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
    if not PINNED_CORE_HANDLES_LINKED_CHANNEL:
        skips.append(
            (
                "needs_linked_core",
                pytest.mark.skip(
                    reason="the pinned Core ignores the notifications job; py-05 "
                    "pins one that delivers it"
                ),
            )
        )
    for item in items:
        for marker, skip in skips:
            if item.get_closest_marker(marker):
                item.add_marker(skip)
