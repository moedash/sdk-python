import pytest

_ENVIRONMENTS_WITHOUT_CHANNELS = ("local", "time-skipping", "envconfig")

# The Core the bridge pins decides whether a workflow's subscribe command
# reaches the server. The protos-only pin refuses it; the delivery pin that
# the native layers move to handles it.
PINNED_CORE_HANDLES_CHANNEL_COMMAND = False

# A channel linked to a workflow needs no command, but a notification reaches
# workflow code as the `NotificationsReceived` job Core builds from the
# scheduled event, and the protos-only pin ignores that job. So a linked case
# in which the workflow receives waits for the delivery pin as well; one that
# only talks to the server from the client runs on every layer.
PINNED_CORE_HANDLES_LINKED_CHANNEL = False

# The unsubscribe is a command like the subscribe, refused by the protos-only
# pin and matched against its event by the delivery pin.
PINNED_CORE_HANDLES_UNSUBSCRIBE = False

# A native stream lives on the server, and only the native layers carry a
# provider that opens one; the providers here hold streams in memory or in a
# workflow's History. A case that produces to a native stream waits for the
# layer with that provider.
PINNED_LAYER_HOSTS_NATIVE_STREAMS = False


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
    config.addinivalue_line(
        "markers",
        "needs_channel_core: the case needs the pinned Core to handle the "
        "subscribe-notification-channel command",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_server: the case needs a server that serves channels linked "
        "to a workflow, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_linked_core: the case needs the pinned Core to hand a linked "
        "channel's notifications to workflow code",
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
        "needs_unsubscribe_core: the case needs the pinned Core to handle the "
        "unsubscribe-notification-channel command",
    )
    config.addinivalue_line(
        "markers",
        "needs_stream_channel_server: the case needs a server on which a native "
        "stream notifies the channel named by the stream, named with -E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_execution_server: the case needs a server that addresses a linked "
        "channel by execution, a standalone activity's included, named with "
        "-E host:port",
    )
    config.addinivalue_line(
        "markers",
        "needs_native_provider: the case needs a provider that opens native streams "
        "on the server, which only the native layers carry",
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
    if config.getoption("--workflow-environment") in _ENVIRONMENTS_WITHOUT_CHANNELS:
        for marker, what in (
            ("needs_describe_server", "lists channel subscriptions on describe"),
            ("needs_unsubscribe_server", "accepts the unsubscribe command"),
            ("needs_stream_channel_server", "notifies a stream's channel"),
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
    if not PINNED_CORE_HANDLES_UNSUBSCRIBE:
        skips.append(
            (
                "needs_unsubscribe_core",
                pytest.mark.skip(
                    reason="the pinned Core refuses the unsubscribe command; py-05 "
                    "pins one that handles it"
                ),
            )
        )
    if not PINNED_LAYER_HOSTS_NATIVE_STREAMS:
        skips.append(
            (
                "needs_native_provider",
                pytest.mark.skip(
                    reason="this layer carries no provider for native streams; the "
                    "native layers and the union do"
                ),
            )
        )
    for item in items:
        for marker, skip in skips:
            if item.get_closest_marker(marker):
                item.add_marker(skip)
