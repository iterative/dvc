import os
import signal
import subprocess
import threading
from functools import cache

from packaging.version import InvalidVersion, Version

from dvc.log import logger
from dvc.utils import fix_env

from .decorators import unlocked_repo
from .exceptions import StageCmdFailedError

logger = logger.getChild(__name__)


@cache
def _fish_supports_no_config(executable) -> bool:
    """
    Check if the fish shell supports the --no-config option.

    Parameters:
    executable (str): The path to the fish shell executable.

    Returns:
    bool: True if the fish version is greater than 3.3.0, False otherwise.
    """
    try:
        output = subprocess.check_output(  # noqa: S603
            [executable, "--version"],
            text=True,
        )
        version = Version(output.split(" ")[-1].strip())
        version_to_check = Version("3.3.0")
        return version >= version_to_check
    except (subprocess.CalledProcessError, IndexError, InvalidVersion):
        logger.trace("could not check fish version, defaulting to False")
        return False


def _warn_if_fish(executable):
    if (
        executable is None
        or os.path.basename(executable) != "fish"
        or _fish_supports_no_config(executable)
    ):
        return
    logger.warning(
        "DVC detected that you are using a version of fish shell below 3.3.0 "
        "Be aware that it might cause problems by overwriting "
        "your current environment variables with values defined "
        "in 'config.fish', which might affect your command. See "
        "https://github.com/iterative/dvc/issues/1307. "
    )


def _make_cmd(executable, cmd):
    if executable is None:
        return cmd
    opts = {
        "zsh": ["--no-rcs"],
        "bash": ["--noprofile", "--norc"],
        "fish": [],
    }
    name = os.path.basename(executable).lower()
    opt = opts.get(name, [])
    if name == "fish" and _fish_supports_no_config(executable):
        opt.append("--no-config")
    return [executable, *opt, "-c", cmd]


def _enforce_cmd_list(cmd):
    assert cmd
    return cmd if isinstance(cmd, list) else cmd.splitlines()


def prepare_kwargs(stage, run_env=None):
    from dvc.env import DVC_ROOT, DVC_STAGE

    kwargs = {"cwd": stage.wdir, "env": fix_env(None), "close_fds": True}

    if run_env:
        kwargs["env"].update(run_env)
    if DVC_ROOT not in kwargs["env"]:
        kwargs["env"][DVC_ROOT] = stage.repo.root_dir

    # Create DVC_STAGE env variable for every command
    kwargs["env"][DVC_STAGE] = stage.addressing

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
    kwargs["shell"] = os.name == "nt"
    return kwargs


def display_command(cmd):
    logger.info("%s %s", ">", cmd)


def get_executable():
    return (os.getenv("SHELL") or "/bin/sh") if os.name != "nt" else None


def _run(executable, cmd, **kwargs):
    main_thread = isinstance(
        threading.current_thread(),
        threading._MainThread,  # type: ignore[attr-defined]
    )
    old_handler = None

    exec_cmd = _make_cmd(executable, cmd)

    try:
        p = subprocess.Popen(exec_cmd, **kwargs)  # noqa: S603
        if main_thread:
            old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        p.communicate()

        if p.returncode != 0:
            raise StageCmdFailedError(cmd, p.returncode)
    finally:
        if old_handler:
            signal.signal(signal.SIGINT, old_handler)


def cmd_run(stage, dry=False, run_env=None):
    logger.info("Running stage '%s':", stage.addressing)
    commands = _enforce_cmd_list(stage.cmd)
    kwargs = prepare_kwargs(stage, run_env=run_env)
    executable = get_executable()

    if not dry:
        _warn_if_fish(executable)

    for cmd in commands:
        display_command(cmd)
        if dry:
            continue

        _run(executable, cmd, **kwargs)


def _pull_missing_deps(stage):
    from dvc.dependency import DatasetDependency, DbDependency

    for dep in stage.deps:
        if isinstance(dep, (DatasetDependency, DbDependency)):
            continue
        if not dep.exists:
            stage.repo.pull(dep.def_path)


def run_stage(stage, dry=False, force=False, run_env=None, **kwargs):
    if not force:
        if kwargs.get("pull") and not dry:
            _pull_missing_deps(stage)

        from .cache import RunCacheNotFoundError

        try:
            stage.repo.stage_cache.restore(stage, dry=dry, **kwargs)
            if not dry:
                return
        except RunCacheNotFoundError:
            if not dry:
                stage.save_deps()

    run = cmd_run if dry else unlocked_repo(cmd_run)
    run(stage, dry=dry, run_env=run_env)
