# Generated file. DO NOT EDIT
"""Generated calls for Core's in-process StreamService."""

from __future__ import annotations

from typing import TYPE_CHECKING

import temporalio.bridge.proto.streams

if TYPE_CHECKING:
    from temporalio.bridge.streams import StreamStore


class StreamService:
    """Calls for the StreamService, which Core dispatches in process."""

    def __init__(self, store: StreamStore) -> None:
        """Initialize service with the provided store."""
        self._store = store

    async def append(
        self, req: temporalio.bridge.proto.streams.AppendRequest
    ) -> temporalio.bridge.proto.streams.AppendResponse:
        """Invokes the StreamService.append call."""
        return await self._store.call(
            "Append", req, temporalio.bridge.proto.streams.AppendResponse
        )

    async def close(
        self, req: temporalio.bridge.proto.streams.CloseRequest
    ) -> temporalio.bridge.proto.streams.CloseResponse:
        """Invokes the StreamService.close call."""
        return await self._store.call(
            "Close", req, temporalio.bridge.proto.streams.CloseResponse
        )

    async def delete_owner(
        self, req: temporalio.bridge.proto.streams.DeleteOwnerRequest
    ) -> temporalio.bridge.proto.streams.DeleteOwnerResponse:
        """Invokes the StreamService.delete_owner call."""
        return await self._store.call(
            "DeleteOwner", req, temporalio.bridge.proto.streams.DeleteOwnerResponse
        )

    async def latest(
        self, req: temporalio.bridge.proto.streams.LatestRequest
    ) -> temporalio.bridge.proto.streams.LatestResponse:
        """Invokes the StreamService.latest call."""
        return await self._store.call(
            "Latest", req, temporalio.bridge.proto.streams.LatestResponse
        )

    async def read(
        self, req: temporalio.bridge.proto.streams.ReadRequest
    ) -> temporalio.bridge.proto.streams.ReadResponse:
        """Invokes the StreamService.read call."""
        return await self._store.call(
            "Read", req, temporalio.bridge.proto.streams.ReadResponse
        )
