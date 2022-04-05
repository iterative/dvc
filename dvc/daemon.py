"""Launch `dvc daemon` command in a separate detached process."""

import inspect
import logging
import os
import platform
import sys
from subprocess import Popen

from dvc.env import DVC_DAEMON
from dvc.utils import fix_env, is_binary

logger = logging.getLogger(__name__)


def _popen(cmd, **kwargs):
    prefix = [sys.executable]
    if not is_binary():
        main_entrypoint = os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "__main__.py"
        )
        prefix += [main_entrypoint]
    return Popen(prefix + cmd, close_fds=True, shell=False, **kwargs)


def _spawn_windows(cmd, env):
    from subprocess import (
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

    _popen(cmd, env=env, creationflags=creationflags, startupinfo=startupinfo)


def _spawn_posix(cmd, env):
    from dvc.cli import main

    # NOTE: using os._exit instead of sys.exit, because dvc built
    # with PyInstaller has trouble with SystemExit exception and throws
    # errors such as "[26338] Failed to execute script __main__"
    try:
        pid = os.fork()  # pylint: disable=no-member
        if pid > 0:
            return
    except OSError:
        logger.exception("failed at first fork")
        os._exit(1)  # pylint: disable=protected-access

    os.setsid()  # pylint: disable=no-member

    try:
        pid = os.fork()  # pylint: disable=no-member
        if pid > 0:
            os._exit(0)  # pylint: disable=protected-access
    except OSError:
        logger.exception("failed at second fork")
        os._exit(1)  # pylint: disable=protected-access

    sys.stdin.close()
    sys.stdout.close()
    sys.stderr.close()

    if platform.system() == "Darwin":
        # workaround for MacOS bug
        # https://github.com/iterative/dvc/issues/4294
        _popen(cmd, env=env).communicate()
    else:
        os.environ.update(env)
        main(cmd)

    os._exit(0)  # pylint: disable=protected-access


def _spawn(cmd, env):
    logger.debug(f"Trying to spawn '{cmd}'")

    if os.name == "nt":
        _spawn_windows(cmd, env)
    elif os.name == "posix":
        _spawn_posix(cmd, env)
    else:
        raise NotImplementedError

    logger.debug(f"Spawned '{cmd}'")


def daemon(args):
    """Launch a `dvc daemon` command in a detached process.

    Args:
        args (list): list of arguments to append to `dvc daemon` command.
    """
    if os.environ.get(DVC_DAEMON):
        logger.debug("skipping launching a new daemon.")
        return

    cmd = ["daemon", "-q"] + args

    env = fix_env()
    if not is_binary():
        file_path = os.path.abspath(inspect.stack()[0][1])
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(file_path))
    env[DVC_DAEMON] = "1"

    _spawn(cmd, env)
