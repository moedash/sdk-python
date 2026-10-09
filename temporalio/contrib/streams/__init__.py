"""Streams: an ordered log a Workflow owns, written from many places.

.. warning::
    This package is experimental and may change in future versions.

A Workflow owns a stream. Producers outside Workflow code, such as the
Workflow's Activities and clients, append to it, and outside readers consume
it from a cursor. Records live in a store the application runs, reached
through a provider, and never pass through Temporal or its History.

The record on the wire is ``temporal.sdk.streams.v1.StreamRecord``, in
:mod:`temporalio.contrib.streams.proto.v1`, with the user's value in ``body``
as an ordinary payload.
"""
