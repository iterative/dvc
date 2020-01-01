"""Launch `dvc daemon` command in a separate detached process."""

import inspect
import logging
import os
import sys
from subprocess import Popen

from dvc.env import DVC_DAEMON
from dvc.utils import fix_env
from dvc.utils import is_binary


logger = logging.getLogger(__name__)

CREATE_NEW_PROCESS_GROUP = 0x00000200
DETACHED_PROCESS = 0x00000008


def _spawn_windows(cmd, env):
    from subprocess import STARTUPINFO, STARTF_USESHOWWINDOW

    creationflags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS

    startupinfo = STARTUPINFO()
    startupinfo.dwFlags |= STARTF_USESHOWWINDOW

    Popen(
        cmd,
        env=env,
        close_fds=True,
        shell=False,
        creationflags=creationflags,
        startupinfo=startupinfo,
    ).communicate()


def _spawn_posix(cmd, env):
    # NOTE: using os._exit instead of sys.exit, because dvc built
    # with PyInstaller has trouble with SystemExit exception and throws
    # errors such as "[26338] Failed to execute script __main__"
    try:
        pid = os.fork()
        if pid > 0:
            return
    except OSError:
        logger.exception("failed at first fork")
        os._exit(1)  # pylint: disable=protected-access

    os.setsid()
    os.umask(0)

    try:
        pid = os.fork()
        if pid > 0:
            os._exit(0)  # pylint: disable=protected-access
    except OSError:
        logger.exception("failed at second fork")
        os._exit(1)  # pylint: disable=protected-access

    sys.stdin.close()
    sys.stdout.close()
    sys.stderr.close()

    Popen(cmd, env=env, close_fds=True, shell=False).communicate()

    os._exit(0)  # pylint: disable=protected-access


def _spawn(cmd, env):
    logger.debug("Trying to spawn '{}'".format(cmd))

    if os.name == "nt":
        _spawn_windows(cmd, env)
    elif os.name == "posix":
        _spawn_posix(cmd, env)
    else:
        raise NotImplementedError

    logger.debug("Spawned '{}'".format(cmd))


def daemon(args):
    """Launch a `dvc daemon` command in a detached process.

    Args:
        args (list): list of arguments to append to `dvc daemon` command.
    """
    if os.environ.get(DVC_DAEMON):
        logger.debug("skipping launching a new daemon.")
        return

    cmd = [sys.executable]
    if not is_binary():
        cmd += [sys.argv[0]]
    cmd += ["daemon", "-q"] + args

    env = fix_env()
    file_path = os.path.abspath(inspect.stack()[0][1])
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(file_path))
    env[DVC_DAEMON] = "1"

    _spawn(cmd, env)
