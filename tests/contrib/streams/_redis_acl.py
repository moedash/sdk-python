"""A Redis ACL user with exactly the rules the guarantees page documents."""

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import redis.asyncio

GUARANTEES = (
    Path(__file__).parents[3]
    / "temporalio"
    / "contrib"
    / "streams"
    / "docs"
    / "redis-guarantees.md"
)

_DOC_KEYS = "~temporal-streams:{my-ns:*"


def documented_rules() -> list[str]:
    """The rules of the page's ``ACL SETUSER`` line, after the user name."""
    lines = re.findall(r"^ACL SETUSER (\S+) (.+)$", GUARANTEES.read_text(), re.M)
    assert len(lines) == 1, "the guarantees page should give one ACL SETUSER line"
    return lines[0][1].split()


@asynccontextmanager
async def documented_user(
    url: str, namespace: str, key_prefix: str, *extra: str
) -> AsyncIterator[str]:
    """Create the documented user for one prefix and namespace; yield its URL.

    The page's key pattern and password are swapped for this test's own, and
    nothing else changes. ``extra`` adds rules the page names separately.
    """
    rules = documented_rules()
    assert _DOC_KEYS in rules and ">secret" in rules
    name = f"streams-acl-{uuid.uuid4().hex}"
    password = uuid.uuid4().hex
    # Key names percent-encode each part, as the provider writes them.
    keys = f"~{quote(key_prefix, safe='')}:{{{quote(namespace, safe='')}:*"
    rules = [
        keys if r == _DOC_KEYS else f">{password}" if r == ">secret" else r
        for r in rules
    ]
    admin = redis.asyncio.Redis.from_url(url)
    await admin.execute_command("ACL", "SETUSER", name, *rules, *extra)
    parts = urlsplit(url)
    netloc = f"{name}:{password}@{parts.hostname}:{parts.port or 6379}"
    try:
        yield urlunsplit(parts._replace(netloc=netloc))
    finally:
        await admin.execute_command("ACL", "DELUSER", name)
        await admin.aclose()
