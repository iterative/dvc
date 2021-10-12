"""DVC re-implementation of fsspec's dedicated async event loop."""
import asyncio
import os
import threading
from typing import List, Optional

from fsspec.asyn import (  # noqa: F401, pylint:disable=unused-import
    _selector_policy,
    sync,
    sync_wrapper,
)

# dedicated async IO thread
iothread: List[Optional[threading.Thread]] = [None]
# global DVC event loop
default_loop: List[Optional[asyncio.AbstractEventLoop]] = [None]
lock = threading.Lock()


def get_loop() -> asyncio.AbstractEventLoop:
    """Create or return the global DVC event loop."""
    if default_loop[0] is None:
        with lock:
            if default_loop[0] is None:
                with _selector_policy():
                    default_loop[0] = asyncio.new_event_loop()
                loop = default_loop[0]
                th = threading.Thread(
                    target=loop.run_forever,  # type: ignore[attr-defined]
                    name="dvcIO",
                )
                th.daemon = True
                th.start()
                iothread[0] = th
    assert default_loop[0] is not None
    return default_loop[0]


class BaseAsyncObject:
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._loop: asyncio.AbstractEventLoop = loop or get_loop()
        self._pid = os.getpid()

    @property
    def loop(self):
        # AsyncMixin is not fork-safe
        assert self._pid == os.getpid()
        return self._loop
