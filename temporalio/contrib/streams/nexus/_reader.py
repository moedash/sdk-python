"""Read the stream a Nexus operation returns, from a Workflow.

A stream-returning operation stays open while its stream runs. Its handler
reports progress whenever the stream moves and completes the operation when
the stream closes; the operation token names the stream. The reader turns
that into batches of typed records: it reads from its cursor through the
stream service on a Nexus endpoint, and waits for progress once a read finds
nothing new.

Every read is a Nexus operation of the Workflow, so History records what each
read answered, and a replay hands over the same batches at the same points.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.contrib.streams._errors import StreamError
from temporalio.contrib.streams._record import Cursor, RecordKind, Supersession
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import RecordDecoder, WireRecord
from temporalio.contrib.streams.nexus._generated import ReadInput, TemporalStreams
from temporalio.contrib.streams.nexus._operation import stream_ref_from_token

__all__ = ["ReadSupersession", "StreamReader", "StreamRecordError"]

T = TypeVar("T")

_BATCH = 100
# Once the operation completes, a read waits this long for a record, so a
# reader waiting for the stream's end does not spin.
_DRAIN_WAIT_MS = 2000


class StreamRecordError(StreamError):
    """A record the reader could not turn into an item.

    .. warning::
        This API is experimental and may change in future versions.

    The reader's cursor is already past the record, so the next call to
    :meth:`StreamReader.next` goes on after it. A body that fails to decode
    usually means the Workers do not share a payload codec, or the item type
    does not match what the producer wrote.
    """

    def __init__(self, message: str, *, cursor: Cursor) -> None:
        """A record at ``cursor`` that could not be decoded."""
        super().__init__(message)
        self.cursor = cursor
        """Where the record is in the stream."""


@dataclass(frozen=True)
class ReadSupersession:
    """Where a reader saw a later attempt of a producer take over.

    .. warning::
        This API is experimental and may change in future versions.
    """

    index: int
    """How many items the reader had handed over before it. The items from
    there on that the producer wrote belong to the later attempt; the ones
    before it from the same producer may be replaced by them."""

    cursor: Cursor
    """The stream position it reports, just before the later attempt's first
    record."""

    supersession: Supersession
    """The producer and the attempts it names."""


class StreamReader(Generic[T]):
    """Reads the stream a Nexus operation returns, in a Workflow.

    .. warning::
        This API is experimental and may change in future versions.

    Make one from the operation's handle and call :meth:`next` until it
    returns ``None``::

        reader = StreamReader(handle, item_type=Token, endpoint="chat")
        while (batch := await reader.next()) is not None:
            ...

    A generated client's ``read_<operation>_stream`` method builds one with
    the item type and endpoint the contract and the client know.

    The reader holds one cursor, starts at the oldest record the stream
    retains, and hands over each record once. Only data records are handed
    over; a producer's ``FINISH`` record is not. When a later attempt of a
    producer takes over (a retried Activity, for example), the earlier
    attempt's records already handed over stay handed over, and
    :attr:`supersessions` says where the later attempt began. A record whose
    body the Workflow's payload converter cannot decode into ``item_type``
    raises :class:`StreamRecordError` from :meth:`next` once the records
    before it are handed over; the call after that goes on past it.

    Bodies reach Workflow code decoded: the stream service decodes each body
    with its Worker's data converter, payload codec included, and the read's
    result travels back as one Nexus operation result, which this Worker's
    codec decodes. So the Workers on both sides need the same payload codec,
    as Workers in one namespace normally have.
    """

    def __init__(
        self,
        handle: workflow.NexusOperationHandle[Any],
        *,
        item_type: type[T],
        endpoint: str,
    ) -> None:
        """Read the stream ``handle``'s operation returns.

        Args:
            handle: The started stream-returning operation. Its token names
                the stream.
            item_type: What each record's body decodes into.
            endpoint: The Nexus endpoint that serves the stream service for
                this stream, usually the operation's own.
        """
        self._handle = handle
        self._item_type = item_type
        self._endpoint = endpoint
        self._stream: StreamRef | None = None
        self._decoder: RecordDecoder | None = None
        self._cursor = ""
        self._counter = 0
        # Whether the last read answered with no record. An answer short of
        # the batch may still leave records behind (the service caps an
        # answer's bytes too), so only an empty one means the tail.
        self._caught_up = False
        # The stream service said no record will follow.
        self._stream_done = False
        self._operation_resolved = False
        self._operation_failed = False
        self._ended = False
        self._handed_over = 0
        self._supersessions: list[ReadSupersession] = []
        self._undecodable: StreamRecordError | None = None
        self._decode_failure: str | None = None
        self._last_warning = ""

    @property
    def supersessions(self) -> Sequence[ReadSupersession]:
        """Every producer attempt change this reader has seen, in order."""
        return tuple(self._supersessions)

    async def next(self) -> list[T] | None:
        """The next batch of records, or ``None`` once the stream has ended.

        Reads on while reads answer with records, and waits for the
        operation to report progress only once a read answers with none.
        Never answers with an empty batch. The stream closes in the store
        before the operation completes, so once the operation completes the
        reader reads until the stream service says the stream is done, then
        answers ``None``. An operation that failed never closed the stream:
        the reader hands over what a read finds, until a read that waited a
        moment answers with none, then raises the failure.

        Raises:
            temporalio.exceptions.NexusOperationError: A read failed, or the
                operation failed. A failed operation raises after the records
                it left are handed over.
            StreamRecordError: A record could not be decoded into
                ``item_type``. The next call goes on past it.
            ValueError: The operation's token does not name a stream.
        """
        if self._undecodable is not None:
            error, self._undecodable = self._undecodable, None
            raise error
        while not self._ended:
            # After a completion, only the stream's end ends the read.
            reads_on = self._operation_resolved and not self._operation_failed
            if not self._stream_done and (reads_on or not self._caught_up):
                batch = await self._read()
                if batch:
                    self._handed_over += len(batch)
                    return batch
                if self._undecodable is not None:
                    error, self._undecodable = self._undecodable, None
                    raise error
                # An answer of records that carry no item, such as a FINISH,
                # is not the tail.
                continue
            if self._operation_resolved:
                self._ended = True
                # Raises the operation's failure, if it failed.
                await self._handle
                return None
            progress = await self._handle.progress(after_counter=self._counter)
            if progress is None:
                # Records may have landed after the last progress.
                self._operation_resolved = True
                try:
                    await self._handle
                except Exception:
                    self._operation_failed = True
            else:
                self._counter = progress.counter
            self._caught_up = False
        return None

    async def _read(self) -> list[T]:
        if self._stream is None:
            token = self._handle.operation_token
            if not token:
                raise ValueError("the operation has no token, so it names no stream")
            self._stream = stream_ref_from_token(token)
            self._decoder = RecordDecoder(
                workflow.payload_converter(),
                self._item_type,
                after=Cursor(self._cursor),
                warn=self._warn,
            )
        assert self._decoder is not None
        # A read covers every record the progress seen so far announced.
        latest = self._handle.latest_progress
        if latest is not None:
            self._counter = max(self._counter, latest.counter)
        client = workflow.create_nexus_client(
            service=TemporalStreams, endpoint=self._endpoint
        )
        answer = await client.execute_operation(
            TemporalStreams.read,
            ReadInput(
                stream=self._stream,
                after_token=self._cursor or None,
                max_records=_BATCH,
                wait_ms=_DRAIN_WAIT_MS if self._operation_resolved else 0,
            ),
        )
        batch: list[T] = []
        for stored in answer.records:
            cursor = Cursor(stored.token)
            try:
                wire = WireRecord.FromString(stored.record)
            except DecodeError as error:
                self._decode_failure = str(error)
            else:
                records = self._decoder.decode(cursor, wire)
                if not records:
                    # The decoder hands back nothing only for a record it
                    # could not decode, after warning why.
                    self._decode_failure = self._last_warning
                for record in records:
                    if record.kind is RecordKind.DATA:
                        batch.append(cast(T, record.value))
                    elif record.supersession is not None:
                        self._supersessions.append(
                            ReadSupersession(
                                index=self._handed_over + len(batch),
                                cursor=record.cursor,
                                supersession=record.supersession,
                            )
                        )
            self._cursor = stored.token
            if self._decode_failure is not None:
                # Stop at the bad record; what follows it is read again next.
                self._undecodable = StreamRecordError(
                    f"stream record at {cursor} could not be read as "
                    f"{getattr(self._item_type, '__name__', self._item_type)!s}: "
                    f"{self._decode_failure}",
                    cursor=cursor,
                )
                self._decode_failure = None
                self._caught_up = False
                return batch
        if answer.next_token:
            self._cursor = answer.next_token
        self._caught_up = not answer.records
        self._stream_done = answer.done
        return batch

    def _warn(self, message: str) -> None:
        self._last_warning = message
        workflow.logger.warning(message)
