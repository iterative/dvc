import json
import logging
import os
import shlex
import subprocess
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass
from typing import List, Optional, TextIO, Union

from funcy import cached_property
from shortuuid import uuid

from dvc.utils.fs import makedirs

from .exceptions import TimeoutExpired

logger = logging.getLogger(__name__)


@dataclass
class ProcessInfo:
    pid: int
    stdin: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]
    returncode: Optional[int]

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def asdict(self):
        return asdict(self)


class ManagedProcess(AbstractContextManager):
    """Run the specified command with redirected output.

    stdout and stderr will both be redirected to <name>.out.
    Interactive processes (requiring stdin input) are currently unsupported.

    Parameters:
        args: Command to be run.
        wdir: If specified, redirected output files will be placed in `wdir`.
        name: Name to use for this process, if not specified a UUID will be
            generated instead.
    """

    def __init__(
        self,
        args: Union[str, List[str]],
        wdir: Optional[str] = None,
        name: Optional[str] = None,
    ):
        self.args: List[str] = (
            shlex.split(args, posix=os.name == "posix")
            if isinstance(args, str)
            else list(args)
        )
        self.wdir = wdir
        self.name = name or uuid()
        self.returncode: Optional[int] = None
        self._stdout: Optional[TextIO] = None
        self._stderr: Optional[TextIO] = None

        self._run()

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()

    def _close_fds(self):
        if self._stdout:
            self._stdout.close()
            self._stdout = None
        if self._stderr:
            self._stderr.close()
            self._stderr = None

    def _make_path(self, path: str) -> str:
        return os.path.join(self.wdir, path) if self.wdir else path

    @cached_property
    def stdout_path(self) -> str:
        return self._make_path(f"{self.name}.out")

    @cached_property
    def info_path(self) -> str:
        return self._make_path(f"{self.name}.json")

    @cached_property
    def pidfile_path(self) -> str:
        return self._make_path(f"{self.name}.pid")

    @property
    def info(self) -> "ProcessInfo":
        return ProcessInfo(
            pid=self.pid,
            stdin=None,
            stdout=self.stdout_path,
            stderr=None,
            returncode=self.returncode,
        )

    def _make_wdir(self):
        if self.wdir:
            makedirs(self.wdir, exist_ok=True)

    def _dump(self):
        self._make_wdir()
        with open(self.info_path, "w", encoding="utf-8") as fobj:
            json.dump(self.info.asdict(), fobj)
        with open(self.pidfile_path, "w", encoding="utf-8") as fobj:
            fobj.write(str(self.pid))

    def _run(self):
        self._make_wdir()
        logger.debug(
            "Appending output to '%s'",
            self.stdout_path,
        )
        self._stdout = open(self.stdout_path, "ab")
        try:
            self._proc = subprocess.Popen(
                self.args,
                stdin=subprocess.DEVNULL,
                stdout=self._stdout,
                stderr=subprocess.STDOUT,
                close_fds=True,
                shell=False,
            )
            self.pid: int = self._proc.pid
            self._dump()
        except Exception:
            if self._proc is not None:
                self._proc.kill()
            self._close_fds()
            raise

    def wait(self, timeout: Optional[int] = None) -> Optional[int]:
        """Block until a process started with `run` has completed.

        Raises:
            TimeoutExpired if `timeout` was set and the process
            did not terminate after `timeout` seconds.
        """
        if self.returncode is not None:
            return self.returncode
        try:
            self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise TimeoutExpired(exc.cmd, exc.timeout) from exc
        self.returncode = self._proc.returncode
        self._close_fds()
        self._dump()
        return self.returncode

    @classmethod
    def spawn(cls, *args, **kwargs) -> Optional[int]:
        """Spawn a ManagedProcess command in the background.

        Returns: The spawned process PID.
        """
        import multiprocessing as mp

        proc = mp.Process(
            target=cls._spawn,
            args=args,
            kwargs=kwargs,
            daemon=True,
        )
        proc.start()
        # Do not terminate the child daemon when the main process exits
        # pylint: disable=protected-access
        mp.process._children.discard(proc)  # type: ignore[attr-defined]
        return proc.pid

    @classmethod
    def _spawn(cls, *args, **kwargs):
        with cls(*args, **kwargs):
            pass
