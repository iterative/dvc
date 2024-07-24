"""Launch `dvc daemon` command in a separate detached process."""

import inspect
import logging
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from typing import TYPE_CHECKING, Any, Optional, Union

from dvc.log import logger

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

from dvc.env import DVC_DAEMON, DVC_DAEMON_LOGFILE
from dvc.utils import fix_env, is_binary
from dvc.utils.collections import ensure_list

logger = logger.getChild(__name__)


def _suppress_resource_warning(popen: subprocess.Popen) -> None:
    """Sets the returncode to avoid ResourceWarning when popen is garbage collected."""
    # only use for daemon processes.
    # See https://bugs.python.org/issue38890.
    popen.returncode = 0


def _win_detached_subprocess(args: Sequence[str], **kwargs) -> int:
    assert os.name == "nt"

    from subprocess import (  # type: ignore[attr-defined]
        CREATE_NEW_PROCESS_GROUP,
        CREATE_NO_WINDOW,
        STARTF_USESHOWWINDOW,
        STARTUPINFO,
    )

    # https://stackoverflow.com/a/7006424
    # https://bugs.python.org/issue41619
    creationflags = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW

    startupinfo = STARTUPINFO()
    startupinfo.dwFlags |= STARTF_USESHOWWINDOW
    popen = subprocess.Popen(  # noqa: S603
        args,
        close_fds=True,
        shell=False,
        startupinfo=startupinfo,
        creationflags=creationflags,
        **kwargs,
    )
    _suppress_resource_warning(popen)
    return popen.pid


def _get_dvc_args() -> list[str]:
    args = [sys.executable]
    if not is_binary():
        root_dir = os.path.abspath(os.path.dirname(__file__))
        main_entrypoint = os.path.join(root_dir, "__main__.py")
        args.append(main_entrypoint)
    return args


def _fork_process() -> int:
    assert os.name == "posix"

    # NOTE: using os._exit instead of sys.exit, because dvc built
    # with PyInstaller has trouble with SystemExit exception and throws
    # errors such as "[26338] Failed to execute script __main__"
    try:
        pid = os.fork()  # type: ignore[attr-defined]
        if pid > 0:
            return pid
    except OSError:
        logger.exception("failed at first fork")
        os._exit(1)

    os.setsid()  # type: ignore[attr-defined]

    try:
        pid = os.fork()  # type: ignore[attr-defined]
        if pid > 0:
            os._exit(0)
    except OSError:
        logger.exception("failed at second fork")
        os._exit(1)

    # disconnect from the terminal
    fd = os.open(os.devnull, os.O_RDWR)
    for fd2 in range(3):
        os.dup2(fd, fd2)
    os.close(fd)
    return pid


def _posix_detached_subprocess(args: Sequence[str], **kwargs) -> int:
    # double fork and execute a subprocess so that there are no zombies
    read_end, write_end = os.pipe()
    pid = _fork_process()
    if pid > 0:  # in parent
        os.close(write_end)
        pid_str = os.read(read_end, 32).decode("utf8")
        os.close(read_end)
        return int(pid_str)

    proc = subprocess.Popen(args, shell=False, close_fds=True, **kwargs)  # noqa: S603
    os.close(read_end)
    os.write(write_end, str(proc.pid).encode("utf8"))
    os.close(write_end)

    exit_code = proc.wait()
    os._exit(exit_code)


def _detached_subprocess(args: Sequence[str], **kwargs) -> int:
    """Run in a detached subprocess."""
    kwargs.setdefault("stdin", subprocess.DEVNULL)
    kwargs.setdefault("stdout", subprocess.DEVNULL)
    kwargs.setdefault("stderr", subprocess.DEVNULL)

    if os.name == "nt":
        return _win_detached_subprocess(args, **kwargs)
    return _posix_detached_subprocess(args, **kwargs)


def _map_log_level_to_flag() -> Optional[str]:
    flags = {logging.DEBUG: "-v", logging.TRACE: "-vv"}  # type: ignore[attr-defined]
    return flags.get(logger.getEffectiveLevel())


def daemon(args: list[str]) -> None:
    """Launch a `dvc daemon` command in a detached process.

    Args:
        args (list): list of arguments to append to `dvc daemon` command.
    """
    if flag := _map_log_level_to_flag():
        args = [*args, flag]
    daemonize(["daemon", *args])


def _spawn(
    args: list[str],
    executable: Optional[Union[str, list[str]]] = None,
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> int:
    file: AbstractContextManager[Any] = nullcontext()
    kwargs = {}
    if output_file:
        file = open(output_file, "ab")  # noqa: SIM115
        kwargs = {"stdout": file, "stderr": file}

    if executable is None:
        executable = _get_dvc_args()
    else:
        executable = ensure_list(executable)

    with file:
        return _detached_subprocess(executable + args, env=env, **kwargs)


def daemonize(args: list[str], executable: Union[None, str, list[str]] = None) -> None:
    if os.name not in ("posix", "nt"):
        return

    if os.environ.get(DVC_DAEMON):
        logger.debug("skipping launching a new daemon.")
        return

    env = fix_env()
    env[DVC_DAEMON] = "1"
    if not is_binary():
        file_path = os.path.abspath(inspect.stack()[0][1])
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(file_path))

    logger.debug("Trying to spawn %r", args)
    pid = _spawn(args, executable, env, output_file=env.get(DVC_DAEMON_LOGFILE))
    logger.debug("Spawned %r with pid %s", args, pid)
