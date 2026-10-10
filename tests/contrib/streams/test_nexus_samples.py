"""The streams over Nexus sample runs end to end.

It needs a server with CHASM, Nexus operation progress and the stream
notifier, for example ``pytest -E 127.0.0.1:7861`` against one built with
those settings. A server without the notifier skips it.
"""

from __future__ import annotations

import uuid

import pytest

from temporalio.api.enums.v1 import StreamOwnerKind
from temporalio.api.stream.v1 import StreamReference
from temporalio.api.workflowservice.v1 import DescribeStreamNotifierRequest
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from tests.contrib.streams.samples import nexus_slack_router
from tests.helpers.nexus import make_nexus_endpoint_name


async def _skip_without_notifier(client: Client) -> None:
    try:
        await client.workflow_service.describe_stream_notifier(
            DescribeStreamNotifierRequest(
                namespace=client.namespace,
                stream_ref=StreamReference(
                    owner_kind=StreamOwnerKind.STREAM_OWNER_KIND_WORKFLOW,
                    workflow_id=f"probe-{uuid.uuid4()}",
                    topic="probe",
                ),
            )
        )
    except RPCError as err:
        if err.status == RPCStatusCode.UNIMPLEMENTED:
            pytest.skip(f"server has no stream notifier: {err.message}")
        # A server that keys the notifier by run chain refuses the probe's
        # empty run id, which still shows it has the notifier.
        if err.status not in (
            RPCStatusCode.NOT_FOUND,
            RPCStatusCode.INVALID_ARGUMENT,
        ):
            raise


async def test_the_slack_router_files_each_message_under_its_channel(
    client: Client, env: WorkflowEnvironment
) -> None:
    await _skip_without_notifier(client)
    task_queue = f"slack-router-{uuid.uuid4()}"
    endpoint = make_nexus_endpoint_name(task_queue)
    await env.create_nexus_endpoint(endpoint, task_queue)

    routed = await nexus_slack_router.main(client, endpoint, task_queue)

    assert routed.channels == {
        "#ops": ["release 42: deploy started", "release 42: deploy finished"],
        "#sales": ["release 42: customer notified"],
    }
    assert routed.summary == "3 messages"
