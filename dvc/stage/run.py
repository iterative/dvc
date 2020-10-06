import logging
import os
import signal
import subprocess
import threading
from contextlib import contextmanager

from dvc.utils import fix_env

from .decorators import relock_repo, unlocked_repo
from .exceptions import StageCmdFailedError

logger = logging.getLogger(__name__)


CHECKPOINT_SIGNAL_FILE = "DVC_CHECKPOINT"


def _nix_cmd(executable, cmd):
    opts = {"zsh": ["--no-rcs"], "bash": ["--noprofile", "--norc"]}
    name = os.path.basename(executable).lower()
    return [executable] + opts.get(name, []) + ["-c", cmd]


def warn_if_fish(executable):
    if (
        executable is None
        or os.path.basename(os.path.realpath(executable)) != "fish"
    ):
        return

    logger.warning(
        "DVC detected that you are using fish as your default "
        "shell. Be aware that it might cause problems by overwriting "
        "your current environment variables with values defined "
        "in '.fishrc', which might affect your command. See "
        "https://github.com/iterative/dvc/issues/1307. "
    )


@unlocked_repo
def cmd_run(stage, *args, checkpoint=False, **kwargs):
    kwargs = {"cwd": stage.wdir, "env": fix_env(None), "close_fds": True}
    if checkpoint:
        # indicate that checkpoint cmd is being run inside DVC
        kwargs["env"].update({"DVC_CHECKPOINT": "1"})

    if os.name == "nt":
        kwargs["shell"] = True
        cmd = stage.cmd
    else:
        # NOTE: when you specify `shell=True`, `Popen` [1] will default to
        # `/bin/sh` on *nix and will add ["/bin/sh", "-c"] to your command.
        # But we actually want to run the same shell that we are running
        # from right now, which is usually determined by the `SHELL` env
        # var. So instead, we compose our command on our own, making sure
        # to include special flags to prevent shell from reading any
        # configs and modifying env, which may change the behavior or the
        # command we are running. See [2] for more info.
        #
        # [1] https://github.com/python/cpython/blob/3.7/Lib/subprocess.py
        #                                                            #L1426
        # [2] https://github.com/iterative/dvc/issues/2506
        #                                           #issuecomment-535396799
        kwargs["shell"] = False
        executable = os.getenv("SHELL") or "/bin/sh"
        warn_if_fish(executable)
        cmd = _nix_cmd(executable, stage.cmd)

    main_thread = isinstance(
        threading.current_thread(),
        threading._MainThread,  # pylint: disable=protected-access
    )
    old_handler = None
    p = None

    try:
        p = subprocess.Popen(cmd, **kwargs)
        if main_thread:
            old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        p.communicate()
    finally:
        if old_handler:
            signal.signal(signal.SIGINT, old_handler)

    retcode = None if not p else p.returncode
    if retcode != 0:
        raise StageCmdFailedError(stage.cmd, retcode)


def run_stage(stage, dry=False, force=False, checkpoint_func=None, **kwargs):
    if not (dry or force or checkpoint_func):
        from .cache import RunCacheNotFoundError

        try:
            stage.repo.stage_cache.restore(stage, **kwargs)
            return
        except RunCacheNotFoundError:
            pass

    callback_str = "callback " if stage.is_callback else ""
    logger.info(
        "Running %s" "stage '%s' with command:",
        callback_str,
        stage.addressing,
    )
    logger.info("\t%s", stage.cmd)
    if not dry:
        with checkpoint_monitor(stage, checkpoint_func) as monitor:
            cmd_run(stage, checkpoint=monitor is not None)


class CheckpointCond:
    def __init__(self):
        self.done = False
        self.cond = threading.Condition()

    def notify(self):
        with self.cond:
            self.done = True
            self.cond.notify()

    def wait(self, timeout=None):
        with self.cond:
            return self.cond.wait(timeout) or self.done


@contextmanager
def checkpoint_monitor(stage, callback_func):
    if not callback_func:
        yield None
        return

    done_cond = CheckpointCond()
    monitor_thread = threading.Thread(
        target=_checkpoint_run, args=(stage, callback_func, done_cond),
    )

    try:
        monitor_thread.start()
        yield monitor_thread
    finally:
        done_cond.notify()
        monitor_thread.join()


def _checkpoint_run(stage, callback_func, done_cond):
    """Run callback_func whenever checkpoint signal file is present."""
    signal_path = os.path.join(stage.repo.tmp_dir, CHECKPOINT_SIGNAL_FILE)
    while True:
        if os.path.exists(signal_path):
            _run_callback(stage, callback_func)
            logger.debug("Remove checkpoint signal file")
            os.remove(signal_path)
        if done_cond.wait(1):
            return


@relock_repo
def _run_callback(stage, callback_func):
    stage.save()
    # TODO: do we need commit() (and check for --no-commit) here
    logger.debug("Running checkpoint callback for stage '%s'", stage)
    callback_func()
