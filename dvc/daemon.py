"""Launch `dvc daemon` command in a separate detached process."""

import inspect
import logging
import os
import platform
import subprocess  # nosec B404
import sys
from contextlib import nullcontext
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from dvc.env import DVC_DAEMON, DVC_DAEMON_LOGFILE
from dvc.utils import fix_env, is_binary
from dvc.utils.collections import ensure_list

if TYPE_CHECKING:
    from typing import IO, ContextManager

logger = logging.getLogger(__name__)

_FILE = Union[None, int, "IO[Any]"]


def _suppress_resource_warning(popen: subprocess.Popen) -> None:
    """Sets the returncode to avoid ResourceWarning when popen is garbage collected."""
    # only use for daemon processes.
    # See https://bugs.python.org/issue38890.
    popen.returncode = 0


def run_detached(
    args: Sequence[str],
    env: Optional[Mapping[str, str]] = None,
    stdin: _FILE = subprocess.DEVNULL,
    stdout: _FILE = subprocess.DEVNULL,
    stderr: _FILE = subprocess.DEVNULL,
    **kwargs: Any,
) -> int:
    # NOTE: this may create zombie processes on unix
    startupinfo = None
    creationflags = 0
    if sys.platform == "win32":
        from subprocess import (  # nosec B404
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

    popen = subprocess.Popen(
        args,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
        close_fds=True,
        shell=False,  # noqa: S603 # nosec B603
        env=env,
        startupinfo=startupinfo,
        creationflags=creationflags,
        start_new_session=True,
        **kwargs,
    )
    _suppress_resource_warning(popen)
    return popen.pid


def _get_dvc_args() -> List[str]:
    args = [sys.executable]
    if not is_binary():
        root_dir = os.path.abspath(os.path.dirname(__file__))
        main_entrypoint = os.path.join(root_dir, "__main__.py")
        args.append(main_entrypoint)
    return args


def _spawn_posix(
    executable: List[str],
    args: List[str],
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> None:
    from dvc.cli import main

    # `fork` will copy buffers, so we need to flush them before forking.
    # Otherwise, we will get duplicated outputs.
    if sys.stdout and not sys.stdout.closed:
        sys.stdout.flush()
    if sys.stderr and not sys.stderr.closed:
        sys.stderr.flush()

    # NOTE: using os._exit instead of sys.exit, because dvc built
    # with PyInstaller has trouble with SystemExit exception and throws
    # errors such as "[26338] Failed to execute script __main__"
    try:
        # pylint: disable-next=no-member
        pid = os.fork()  # type: ignore[attr-defined]
        if pid > 0:
            return
    except OSError:
        logger.exception("failed at first fork")
        os._exit(1)  # pylint: disable=protected-access

    os.setsid()  # type: ignore[attr-defined]  # pylint: disable=no-member

    try:
        # pylint: disable-next=no-member
        pid = os.fork()  # type: ignore[attr-defined]
        if pid > 0:
            os._exit(0)  # pylint: disable=protected-access
    except OSError:
        logger.exception("failed at second fork")
        os._exit(1)  # pylint: disable=protected-access

    # disconnect from the terminal
    fd = os.open(os.devnull, os.O_RDWR)
    os.dup2(fd, sys.stdin.fileno())
    os.close(fd)

    with open(output_file or os.devnull, "ab") as f:
        os.dup2(f.fileno(), sys.stdout.fileno())
        os.dup2(f.fileno(), sys.stderr.fileno())

    if platform.system() == "Darwin":
        # workaround for MacOS bug
        # https://github.com/iterative/dvc/issues/4294
        subprocess.Popen(
            executable + args, env=env, shell=False  # noqa: S603 # nosec B603
        ).communicate()
    else:
        os.environ.update(env or {})
        main(args)

    os._exit(0)  # pylint: disable=protected-access


def daemon(args: Iterable[str]) -> None:
    """Launch a `dvc daemon` command in a detached process.

    Args:
        args (list): list of arguments to append to `dvc daemon` command.
    """
    flags = {
        logging.CRITICAL: "-q",
        logging.DEBUG: "-v",
        logging.TRACE: "-vv",  # type: ignore[attr-defined]
    }
    args = list(args)
    if flag := flags.get(logger.getEffectiveLevel()):
        args.append(flag)
    daemonize(["daemon", *args])


def _spawn_subprocess(
    executable: List[str],
    args: List[str],
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> Optional[int]:
    # adapt run_detached to _spawn's interface
    file: "ContextManager[_FILE]" = nullcontext(subprocess.DEVNULL)
    if output_file:
        file = open(output_file, "ab")  # noqa: SIM115

    with file as f:
        return run_detached(executable + args, env, stdout=f, stderr=f)


def _spawn(
    executable: List[str],
    args: List[str],
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> Optional[int]:
    if os.name == "nt":
        return _spawn_subprocess(executable, args, env, output_file=output_file)

    if os.name == "posix":
        _spawn_posix(executable, args, env, output_file=output_file)
        return None

    raise NotImplementedError


def daemonize(args: List[str], executable: Union[None, str, List[str]] = None) -> None:
    if os.environ.get(DVC_DAEMON):
        logger.debug("skipping launching a new daemon.")
        return

    executable = _get_dvc_args() if executable is None else ensure_list(executable)

    env = fix_env()
    env[DVC_DAEMON] = "1"
    if not is_binary():
        file_path = os.path.abspath(inspect.stack()[0][1])
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(file_path))

    logger.debug("Trying to spawn %r", args)
    pid = _spawn(executable, args, env, output_file=env.get(DVC_DAEMON_LOGFILE))
    logger.debug("Spawned %r%s", args, f" with pid {pid}" if pid else "")
