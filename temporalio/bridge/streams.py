"""Core's stream store, reached in process.

Core serves the stream service against one store per process. Python sends each call as a
serialized request and gets the serialized response back, so the typed calls in
:mod:`temporalio.bridge.streams_generated` are generated from Core's protos.
"""

from __future__ import annotations

from typing import TypeVar

import google.protobuf.message

import temporalio.bridge.client
import temporalio.bridge.proto.streams
import temporalio.bridge.temporal_sdk_bridge

ProtoMessage = TypeVar("ProtoMessage", bound=google.protobuf.message.Message)


class StreamCallFailure(Exception):
    """A stream call that failed, with Core's account of why."""

    def __init__(self, failure: temporalio.bridge.proto.streams.StreamFailure) -> None:
        """Wrap Core's failure."""
        super().__init__(failure.message)
        self.failure = failure


class StreamStore:
    """The stream store Core serves to this process and its Workers."""

    @staticmethod
    async def connect(
        client: temporalio.bridge.client.Client,
        config: temporalio.bridge.proto.streams.StreamStoreConfig,
    ) -> StreamStore:
        """Connect to the store ``config`` names.

        Core asks ``client``'s server about streams' owners.

        Raises:
            StreamCallFailure: Core could not reach the store.
        """
        try:
            ref = await temporalio.bridge.temporal_sdk_bridge.connect_stream_store(
                client._ref, config.SerializeToString()
            )
        except temporalio.bridge.temporal_sdk_bridge.StreamFailureError as error:
            raise _call_failure(error) from None
        return StreamStore(ref)

    def __init__(
        self, ref: temporalio.bridge.temporal_sdk_bridge.StreamStoreRef
    ) -> None:
        """Wrap the bridge's store reference."""
        self.ref = ref

    async def call(
        self,
        rpc: str,
        req: google.protobuf.message.Message,
        resp_type: type[ProtoMessage],
    ) -> ProtoMessage:
        """Make one stream service call and decode its answer.

        Raises:
            StreamCallFailure: The call failed.
        """
        try:
            data = await self.ref.call(rpc, req.SerializeToString())
        except temporalio.bridge.temporal_sdk_bridge.StreamFailureError as error:
            raise _call_failure(error) from None
        resp = resp_type()
        resp.ParseFromString(data)
        return resp


def _call_failure(
    error: temporalio.bridge.temporal_sdk_bridge.StreamFailureError,
) -> StreamCallFailure:
    # The bridge raises with the serialized failure as its only argument.
    failure = temporalio.bridge.proto.streams.StreamFailure()
    failure.ParseFromString(error.args[0])
    return StreamCallFailure(failure)
