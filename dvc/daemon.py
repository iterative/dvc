import os
import sys
from subprocess import Popen
from dvc.logger import Logger


class Daemon(object):  # pragma: no cover
    def _spawn_windows(self, cmd):
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        DETACHED_PROCESS = 0x00000008
        creationflags = CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS
        Popen(cmd,
              close_fds=True,
              shell=False,
              creationflags=creationflags)

    def _spawn_posix(self, cmd):
        try:
            pid = os.fork()
            if pid > 0:
                return
        except OSError as exc:
            Logger.error("Failed at first fork", exc)
            sys.exit(1)

        os.setsid()
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError as exc:
            Logger.error("Failed at second fork", exc)
            sys.exit(1)

        sys.stdin.close()
        sys.stdout.close()
        sys.stderr.close()

        Popen(cmd,
              close_fds=True,
              shell=False)

    def __call__(self, args):
        from dvc.utils import is_binary

        cmd = [sys.executable]
        if not is_binary():
            cmd += ['-m', 'dvc']
        cmd += ['daemon', '-q'] + args

        Logger.debug("Trying to spawn '{}'".format(cmd))

        if os.name == 'nt':
            self._spawn_windows(cmd)
        elif os.name == 'posix':
            self._spawn_posix(cmd)
        else:
            raise NotImplementedError

        Logger.debug("Spawned '{}'".format(cmd))
