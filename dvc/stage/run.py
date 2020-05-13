import logging
import os
import signal
import subprocess
import threading

from dvc.utils import fix_env

from .decorators import unlocked_repo
from .exceptions import StageCmdFailedError

logger = logging.getLogger(__name__)


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
def cmd_run(stage, *args, **kwargs):
    kwargs = {"cwd": stage.wdir, "env": fix_env(None), "close_fds": True}

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

        opts = {"zsh": ["--no-rcs"], "bash": ["--noprofile", "--norc"]}
        name = os.path.basename(executable).lower()
        cmd = [executable] + opts.get(name, []) + ["-c", stage.cmd]

    main_thread = isinstance(threading.current_thread(), threading._MainThread)
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


def run_stage(stage, dry=False, force=False, run_cache=False):
    if dry:
        return
    stage_cache = stage.repo.stage_cache
    stage_cached = (
        not force
        and not stage.is_callback
        and not stage.always_changed
        and stage.already_cached()
    )
    use_build_cache = False
    if not stage_cached:
        stage.save_deps()
        use_build_cache = (
            not force and run_cache and stage_cache.is_cached(stage)
        )

    if use_build_cache:
        # restore stage from build cache
        stage.repo.stage_cache.restore(stage)
        stage_cached = stage.outs_cached()

    if stage_cached:
        logger.info("Stage is cached, skipping.")
        stage.checkout()
    else:
        logger.info("Running command:\n\t{}".format(stage.cmd))
        cmd_run(stage)
