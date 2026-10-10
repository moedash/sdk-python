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
from datetime import datetime, timedelta
from typing import Any, Generic, TypeVar, cast

from google.protobuf.message import DecodeError

from temporalio import workflow
from temporalio.contrib.streams._errors import StreamError, StreamRecordError
from temporalio.contrib.streams._record import Cursor, RecordKind, Supersession
from temporalio.contrib.streams._ref import StreamRef
from temporalio.contrib.streams._wire import RecordDecoder, WireRecord
from temporalio.contrib.streams.nexus._generated import ReadInput, TemporalStreams
from temporalio.contrib.streams.nexus._operation import stream_ref_from_token

__all__ = [
    "ReadSupersession",
    "StreamIncompleteError",
    "StreamReader",
]

T = TypeVar("T")

_BATCH = 100
# Once the operation completes, a read waits this long for a record, so a
# reader waiting for the stream's end does not spin.
_DRAIN_WAIT_MS = 2000


class StreamIncompleteError(StreamError):
    """The operation completed, but the stream never said it ended.

    .. warning::
        This API is experimental and may change in future versions.

    The reader gave up after its drain limit of empty reads, because every
    read is a Nexus operation in the caller's History. It happens when the
    owner's close didn't reach the store, or when the Workflow id moved on to
    a new run chain whose topic is still open.
    """

    def __init__(self, message: str, *, cursor: Cursor) -> None:
        """The reader stopped at ``cursor``."""
        super().__init__(message)
        self.cursor = cursor
        """Where the reader stopped. A new reader can resume after it."""


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
    raises :class:`temporalio.contrib.streams.StreamRecordError` from
    :meth:`next` once the records
    before it are handed over; the call after that goes on past it.

    Each read goes to the run chain the Workflow id has when the read runs,
    not to the chain the operation token names: a stream reference pins a
    run, not a chain. So if the owner's chain ends and the same Workflow id
    starts a new chain while this reader still drains, a later read can hand
    over the new chain's records. Give each stream's owner a Workflow id of
    its own, or let the reader finish before the id is reused.

    Every read is a Nexus operation result in the caller's History, about
    three events and up to one read answer (1 MiB of records) each. To read a
    long stream, Continue-as-New before History grows too large: pass
    :attr:`cursor` to the new run, start the operation again there, and build
    the reader with ``after=cursor``. After a successful completion, the
    reader reads until the stream service says the stream is done, and raises
    :class:`StreamIncompleteError` once its reads have found nothing for
    ``drain_limit``. A read that fails because retention dropped the records
    after the cursor raises with ``StreamExpiredError`` as its cause. A new
    reader with no ``after`` starts again at the oldest record still kept.

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
        after: Cursor | None = None,
        drain_limit: timedelta = timedelta(seconds=60),
    ) -> None:
        """Read the stream ``handle``'s operation returns.

        Args:
            handle: The started stream-returning operation. Its token names
                the stream.
            item_type: What each record's body decodes into.
            endpoint: The Nexus endpoint that serves the stream service for
                this stream, usually the operation's own.
            after: Resume after this cursor, such as a previous run's
                :attr:`cursor`. ``None`` starts at the oldest record kept.
            drain_limit: How long reads after a successful completion may
                find nothing before the reader raises
                :class:`StreamIncompleteError`.

        Raises:
            ValueError: ``drain_limit`` is not positive.
        """
        if drain_limit <= timedelta(0):
            raise ValueError("drain_limit must be positive")
        self._handle = handle
        self._item_type = item_type
        self._endpoint = endpoint
        self._drain_limit = drain_limit
        self._drain_since: datetime | None = None
        self._stream: StreamRef | None = None
        self._decoder: RecordDecoder | None = None
        self._cursor = after.token if after is not None else ""
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

    @property
    def cursor(self) -> Cursor:
        """Where the reader is: past every record it has handed over."""
        return Cursor(self._cursor)

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
            temporalio.contrib.streams.StreamRecordError: A record could not
                be decoded into
                ``item_type``, or was too large to cross. The next call goes
                on past it.
            StreamIncompleteError: The operation completed, but reads found
                nothing for ``drain_limit`` and the stream never ended.
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
                    if reads_on:
                        self._drain_since = workflow.now()
                    return batch
                if self._undecodable is not None:
                    error, self._undecodable = self._undecodable, None
                    raise error
                if (
                    reads_on
                    and self._caught_up
                    and not self._stream_done
                    and self._drain_since is not None
                    and workflow.now() - self._drain_since >= self._drain_limit
                ):
                    raise StreamIncompleteError(
                        f"the operation completed, but the stream didn't end within "
                        f"{self._drain_limit} of empty reads",
                        cursor=self.cursor,
                    )
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
                self._drain_since = workflow.now()
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
            wire: WireRecord | None = None
            if stored.error:
                # The service left out a record no read result can carry.
                self._decode_failure = stored.error
            else:
                try:
                    wire = WireRecord.FromString(stored.record)
                except DecodeError as error:
                    self._decode_failure = str(error)
            if wire is not None:
                try:
                    records = self._decoder.decode(cursor, wire)
                except StreamRecordError as error:
                    self._decode_failure = str(error)
                    records = []
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
                    cursor,
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
        workflow.logger.warning(message)
