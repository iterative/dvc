import functools
import logging
import os
import signal
import subprocess
import threading
from abc import ABC, abstractmethod
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Callable, ClassVar

from funcy import first

from dvc.repo.live import create_summary
from dvc.utils import fix_env

from .decorators import relock_repo, unlocked_repo
from .exceptions import StageCmdFailedError

logger = logging.getLogger(__name__)


class CheckpointKilledError(StageCmdFailedError):
    pass


class LiveKilledError(StageCmdFailedError):
    pass


def _make_cmd(executable, cmd):
    if executable is None:
        return cmd
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


def _enforce_cmd_list(cmd):
    assert cmd
    return cmd if isinstance(cmd, list) else cmd.splitlines()


def prepare_kwargs(stage, checkpoint_func=None, run_env=None):
    kwargs = {"cwd": stage.wdir, "env": fix_env(None), "close_fds": True}

    kwargs["env"].update(stage.env(checkpoint_func=checkpoint_func))
    if run_env:
        kwargs["env"].update(run_env)

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
    kwargs["shell"] = True if os.name == "nt" else False
    return kwargs


def display_command(cmd):
    logger.info("%s %s", ">", cmd)


def get_executable():
    return (os.getenv("SHELL") or "/bin/sh") if os.name != "nt" else None


def _run(stage, executable, cmd, checkpoint_func, **kwargs):
    main_thread = isinstance(
        threading.current_thread(),
        threading._MainThread,  # pylint: disable=protected-access
    )

    exec_cmd = _make_cmd(executable, cmd)
    old_handler = None

    try:
        p = subprocess.Popen(exec_cmd, **kwargs)
        if main_thread:
            old_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)

        monitors = _get_monitors(stage, checkpoint_func, p)

        if monitors:
            with ExitStack() as stack:
                for m in monitors:
                    stack.enter_context(m)
                p.communicate()
        else:
            p.communicate()

        if p.returncode != 0:
            for m in monitors:
                if m.config.killed.is_set():
                    raise m.error_cls(cmd, p.returncode)
            raise StageCmdFailedError(cmd, p.returncode)
    finally:
        if old_handler:
            signal.signal(signal.SIGINT, old_handler)


def _get_monitors(stage, checkpoint_func, proc):
    result = []
    if checkpoint_func:
        result.append(CheckpointMonitor(stage, checkpoint_func, proc))

    live = first((o for o in stage.outs if (o.live and o.live["html"])))
    if live:
        result.append(LiveMonitor(live, proc))

    return result


def cmd_run(stage, dry=False, checkpoint_func=None, run_env=None):
    logger.info("Running stage '%s':", stage.addressing)
    commands = _enforce_cmd_list(stage.cmd)
    kwargs = prepare_kwargs(
        stage, checkpoint_func=checkpoint_func, run_env=run_env
    )
    executable = get_executable()

    if not dry:
        warn_if_fish(executable)

    for cmd in commands:
        display_command(cmd)
        if dry:
            continue

        _run(stage, executable, cmd, checkpoint_func=checkpoint_func, **kwargs)


def run_stage(
    stage, dry=False, force=False, checkpoint_func=None, run_env=None, **kwargs
):
    if not (dry or force or checkpoint_func):
        from .cache import RunCacheNotFoundError

        try:
            stage.repo.stage_cache.restore(stage, **kwargs)
            return
        except RunCacheNotFoundError:
            stage.save_deps()

    run = cmd_run if dry else unlocked_repo(cmd_run)
    run(stage, dry=dry, checkpoint_func=checkpoint_func, run_env=run_env)


@dataclass
class MonitorConfig:
    name: str
    stage: "Stage"  # type: ignore[name-defined] # noqa: F821
    task: Callable
    proc: subprocess.Popen
    done: threading.Event
    killed: threading.Event
    signal_path: str
    AWAIT: ClassVar[float] = 1.0


class Monitor(ABC):
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.monitor_thread = threading.Thread(
            target=_monitor_loop, args=(self.config,),
        )

    def __enter__(self):
        logger.debug(
            "Monitoring stage '%s' with cmd process '%d'",
            self.config.stage,
            self.config.proc.pid,
        )
        self.monitor_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.config.done.set()
        self.monitor_thread.join()

    @property
    @abstractmethod
    def SIGNAL_FILE(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def error_cls(self):
        raise NotImplementedError

    def signal_path(self, stage):
        return os.path.join(stage.repo.tmp_dir, self.SIGNAL_FILE)


class CheckpointMonitor(Monitor):
    SIGNAL_FILE = "DVC_CHECKPOINT"
    error_cls = CheckpointKilledError

    def __init__(self, stage, callback_func, proc):
        super().__init__(
            MonitorConfig(
                "checkpoint",
                stage,
                functools.partial(_run_callback, stage, callback_func),
                proc,
                threading.Event(),
                threading.Event(),
                self.signal_path(stage),
            )
        )


class LiveMonitor(Monitor):
    SIGNAL_FILE = "DVC_LIVE"
    error_cls = LiveKilledError

    def __init__(self, out, proc):
        super().__init__(
            MonitorConfig(
                "live",
                out.stage,
                functools.partial(create_summary, out),
                proc,
                threading.Event(),
                threading.Event(),
                self.signal_path(out.stage),
            )
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure all data is visualied after training
        self.config.task()
        super().__exit__(exc_type, exc_val, exc_tb)


def _monitor_loop(config: MonitorConfig):
    while True:
        if os.path.exists(config.signal_path):
            try:
                config.task()
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "Error running '%s' task, '%s' will be aborted",
                    config.name,
                    config.stage,
                )
                _kill(config.proc)
                config.killed.set()
            finally:
                logger.debug("Remove checkpoint signal file")
                os.remove(config.signal_path)
        if config.done.wait(config.AWAIT):
            return


def _kill(proc):
    if os.name == "nt":
        return _kill_nt(proc)
    proc.terminate()
    proc.wait()


def _kill_nt(proc):
    # windows stages are spawned with shell=True, proc is the shell process and
    # not the actual stage process - we have to kill the entire tree
    subprocess.call(["taskkill", "/F", "/T", "/PID", str(proc.pid)])


@relock_repo
def _run_callback(stage, callback_func):
    stage.save(allow_missing=True)
    stage.commit(allow_missing=True)
    logger.debug("Running checkpoint callback for stage '%s'", stage)
    callback_func()
