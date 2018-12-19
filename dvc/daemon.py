import os
import sys
from subprocess import Popen

from dvc.logger import logger
from dvc.utils import is_binary, fix_env


class Daemon(object):  # pragma: no cover
    def _spawn_windows(self, cmd):
        from subprocess import STARTUPINFO, STARTF_USESHOWWINDOW

        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008
        creationflags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS

        startupinfo = STARTUPINFO()
        startupinfo.dwFlags |= STARTF_USESHOWWINDOW

        p = Popen(cmd,
                  env=fix_env(),
                  close_fds=True,
                  shell=False,
                  creationflags=creationflags,
                  startupinfo=startupinfo)

        p.communicate()

    def _spawn_posix(self, cmd):
        # NOTE: using os._exit instead of sys.exit, because dvc built
        # with PyInstaller has trouble with SystemExit exeption and throws
        # errors such as "[26338] Failed to execute script __main__"
        try:
            pid = os.fork()
            if pid > 0:
                return
        except OSError as exc:
            logger.error("Failed at first fork", exc)
            os._exit(1)

        os.setsid()
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError as exc:
            logger.error("Failed at second fork", exc)
            os._exit(1)

        sys.stdin.close()
        sys.stdout.close()
        sys.stderr.close()

        p = Popen(cmd,
                  env=fix_env(),
                  close_fds=True,
                  shell=False)

        p.communicate()

        os._exit(0)

    def __call__(self, args):
        cmd = [sys.executable]
        if not is_binary():
            cmd += ['-m', 'dvc']
        cmd += ['daemon', '-q'] + args

        logger.debug("Trying to spawn '{}'".format(cmd))

        if os.name == 'nt':
            self._spawn_windows(cmd)
        elif os.name == 'posix':
            self._spawn_posix(cmd)
        else:
            raise NotImplementedError

        logger.debug("Spawned '{}'".format(cmd))
