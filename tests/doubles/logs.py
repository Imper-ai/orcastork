"""Loguru capture — collect records emitted inside a block to assert on levels, kwargs, and exception info."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from loguru import logger


@contextmanager
def capture_logs(level: str = 'WARNING') -> Iterator[list[Any]]:
    """Collect loguru records (dict-like, with ``extra``/``exception`` keys) at or above ``level``."""
    records: list[Any] = []
    handler_id = logger.add(lambda message: records.append(message.record), level=level)
    try:
        yield records
    finally:
        logger.remove(handler_id)
