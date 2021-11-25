"""Serverless process manager."""

import json
import logging
import os
from typing import Generator, List, Optional, Union

from shortuuid import uuid

from .process import ManagedProcess, ProcessInfo

logger = logging.getLogger(__name__)


class ProcessManager:
    """Manager for controlling background ManagedProcess(es).

    Spawned process entries are kept in the manager directory until they
    are explicitly removed (with remove() or cleanup()) so that return
    value and log information can be accessed after a process has completed.
    """

    def __init__(self, wdir: Optional[str] = None):
        self.wdir = wdir or "."

    def __iter__(self):
        return self.processes()

    def __getitem__(self, key: str) -> "ProcessInfo":
        info_path = os.path.join(self.wdir, key, f"{key}.json")
        try:
            with open(info_path, encoding="utf-8") as fobj:
                return ProcessInfo.from_dict(json.load(fobj))
        except FileNotFoundError:
            raise KeyError

    def get(self, key: str, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def processes(self) -> Generator["ProcessInfo", None, None]:
        if not os.path.exists(self.wdir):
            return
        for name in os.listdir(self.wdir):
            try:
                yield self[name]
            except KeyError:
                continue

    def spawn(self, args: Union[str, List[str]], name: Optional[str] = None):
        """Run the given command in the background."""
        name = name or uuid()
        pid = ManagedProcess.spawn(
            args,
            wdir=os.path.join(self.wdir, name),
            name=name,
        )
        logger.debug(
            "Spawned managed process '%s' (PID: '%d')",
            name,
            pid,
        )

    def send_signal(self, name: str, signal: int):
        """Send `signal` to the specified named process."""
        raise NotImplementedError

    def kill(self, name: str):
        """Kill the specified named process."""
        raise NotImplementedError

    def terminate(self, name: str):
        """Terminate the specified named process."""
        raise NotImplementedError

    def remove(self, name: str, force: bool = False):
        """Remove the specified named process from this manager.

        If the specified process is still running, it will be forcefully killed
        if `force` is True`, otherwise an exception will be raised.

        Raises:
            ProcessNotTerminatedError if the specified process is still
            running and was not forcefully killed.
        """
        raise NotImplementedError

    def cleanup(self):
        """Remove stale (terminated) processes from this manager."""
        raise NotImplementedError
