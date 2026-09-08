"""A fakeredis server whose clock the tests control (for lock-TTL / stream-expiry tests).

Mirrors the time-control approach the rest of the suite takes: it monkeypatches the time
function fakeredis reads, so advancing it expires ``PX`` leases deterministically.
"""

from __future__ import annotations

import time
from collections.abc import Callable

import fakeredis._server as _fakeredis_server
from fakeredis import FakeServer


class TimeControlledServer(FakeServer):
    def __init__(self) -> None:
        super().__init__()
        self._now: float = 0.0
        self._original: Callable[[], float] | None = None

    def advance(self, seconds: float) -> None:
        if self._original is None:
            self._now = time.time()
            self._original = _fakeredis_server.time.time
        self._now += seconds
        _fakeredis_server.time.time = lambda: self._now

    def reset(self) -> None:
        if self._original is not None:
            _fakeredis_server.time.time = self._original
            self._original = None
