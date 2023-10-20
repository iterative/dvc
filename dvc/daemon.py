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
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

if TYPE_CHECKING:
    from typing import ContextManager

from dvc.env import DVC_DAEMON, DVC_DAEMON_LOGFILE
from dvc.utils import fix_env, is_binary
from dvc.utils.collections import ensure_list

logger = logging.getLogger(__name__)


def _suppress_resource_warning(popen: subprocess.Popen) -> None:
    """Sets the returncode to avoid ResourceWarning when popen is garbage collected."""
    # only use for daemon processes.
    # See https://bugs.python.org/issue38890.
    popen.returncode = 0


def _win_detached_subprocess(args: Sequence[str], **kwargs) -> int:
    assert os.name == "nt"

    from subprocess import (  # type: ignore[attr-defined] # nosec B404
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
        close_fds=True,
        shell=False,  # noqa: S603 # nosec B603
        startupinfo=startupinfo,
        creationflags=creationflags,
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


def _run_daemon(func: Callable[[], Any], output_file: Optional[str] = None) -> None:
    assert os.name == "posix"

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
    os.dup2(fd, 0)
    os.close(fd)

    with open(output_file or os.devnull, "ab") as f:
        os.dup2(f.fileno(), 1)
        os.dup2(f.fileno(), 2)

    func()
    os._exit(0)  # pylint: disable=protected-access


def _posix_detached_subprocess(*args, **kwargs) -> None:
    kwargs.setdefault("shell", False)
    kwargs.setdefault("close_fds", True)

    def _run_subprocess() -> None:
        subprocess.Popen(*args, **kwargs).communicate()  # noqa: S603 # nosec B603

    # double fork and execute a subprocess so that there are no zombies
    _run_daemon(_run_subprocess)


def _detached_subprocess(*args, **kwargs) -> Optional[int]:
    """Run in a detached subprocess."""

    kwargs.setdefault("stdin", subprocess.DEVNULL)
    kwargs.setdefault("stdout", subprocess.DEVNULL)
    kwargs.setdefault("stderr", subprocess.DEVNULL)

    if os.name == "nt":
        return _win_detached_subprocess(*args, **kwargs)
    _posix_detached_subprocess(*args, **kwargs)
    return None


def _map_log_level_to_flag() -> Optional[str]:
    flags = {
        logging.CRITICAL: "-q",
        logging.DEBUG: "-v",
        logging.TRACE: "-vv",  # type: ignore[attr-defined]
    }
    return flags.get(logger.getEffectiveLevel())


def daemon(args: List[str]) -> None:
    """Launch a `dvc daemon` command in a detached process.

    Args:
        args (list): list of arguments to append to `dvc daemon` command.
    """
    if flag := _map_log_level_to_flag():
        args.append(flag)
    daemonize(["daemon", *args])


def _run_dvc_main_in_daemon(
    args: List[str],
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> None:
    from dvc.cli import main

    def _run_main() -> None:
        os.environ.update(env or {})
        main(args)

    return _run_daemon(_run_main, output_file=output_file)


def _spawn(
    args: List[str],
    executable: Optional[Union[str, List[str]]] = None,
    env: Optional[Mapping[str, str]] = None,
    output_file: Optional[str] = None,
) -> Optional[int]:
    if os.name not in ("posix", "nt"):
        raise NotImplementedError

    if os.name == "posix" and platform.system() != "Darwin":
        _run_dvc_main_in_daemon(args, env, output_file=output_file)
        return None

    file: "ContextManager[Any]" = nullcontext()
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


def daemonize(args: List[str], executable: Union[None, str, List[str]] = None) -> None:
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
    logger.debug("Spawned %r%s", args, f" with pid {pid}" if pid else "")
