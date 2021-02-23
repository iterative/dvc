import functools
import logging
import os
import signal
import subprocess
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

if TYPE_CHECKING:
    from dvc.stage import Stage
    from dvc.output import BaseOutput

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

        tasks = _get_monitor_tasks(stage, checkpoint_func, p)

        if tasks:
            with Monitor(tasks):
                p.communicate()
        else:
            p.communicate()

        if p.returncode != 0:
            for t in tasks:
                if t.killed.is_set():
                    raise t.error_cls(cmd, p.returncode)
            raise StageCmdFailedError(cmd, p.returncode)
    finally:
        if old_handler:
            signal.signal(signal.SIGINT, old_handler)


def _get_monitor_tasks(stage, checkpoint_func, proc):
    result = []
    if checkpoint_func:
        result.append(CheckpointTask(stage, checkpoint_func, proc))

    live = first((o for o in stage.outs if (o.live and o.live["html"])))
    if live:
        result.append(LiveTask(stage, live, proc))

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
class MonitorTask:
    stage: "Stage"
    execute: Callable
    proc: subprocess.Popen
    done: threading.Event = threading.Event()
    killed: threading.Event = threading.Event()

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def SIGNAL_FILE(self) -> str:
        raise NotImplementedError

    @property
    def error_cls(self) -> type:
        raise NotImplementedError

    @property
    def signal_path(self) -> str:
        return os.path.join(self.stage.repo.tmp_dir, self.SIGNAL_FILE)

    def after_run(self):
        pass


class CheckpointTask(MonitorTask):
    name = "checkpoint"
    SIGNAL_FILE = "DVC_CHECKPOINT"
    error_cls = CheckpointKilledError

    def __init__(
        self, stage: "Stage", callback_func: Callable, proc: subprocess.Popen
    ):
        super().__init__(
            stage,
            functools.partial(
                CheckpointTask._run_callback, stage, callback_func
            ),
            proc,
        )

    @staticmethod
    @relock_repo
    def _run_callback(stage, callback_func):
        stage.save(allow_missing=True)
        stage.commit(allow_missing=True)
        logger.debug("Running checkpoint callback for stage '%s'", stage)
        callback_func()


class LiveTask(MonitorTask):
    name = "live"
    SIGNAL_FILE = "DVC_LIVE"
    error_cls = LiveKilledError

    def __init__(
        self, stage: "Stage", out: "BaseOutput", proc: subprocess.Popen
    ):
        super().__init__(stage, functools.partial(create_summary, out), proc)

    def after_run(self):
        # make sure summary is prepared for all the data
        self.execute()


class Monitor:
    AWAIT: float = 1.0

    def __init__(self, tasks: List[MonitorTask]):
        self.done = threading.Event()
        self.tasks = tasks
        self.monitor_thread = threading.Thread(
            target=Monitor._loop, args=(self.tasks, self.done,),
        )

    def __enter__(self):
        self.monitor_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.done.set()
        self.monitor_thread.join()
        for t in self.tasks:
            t.after_run()

    @staticmethod
    def kill(proc):
        if os.name == "nt":
            return Monitor._kill_nt(proc)
        proc.terminate()
        proc.wait()

    @staticmethod
    def _kill_nt(proc):
        # windows stages are spawned with shell=True, proc is the shell process
        # and not the actual stage process - we have to kill the entire tree
        subprocess.call(["taskkill", "/F", "/T", "/PID", str(proc.pid)])

    @staticmethod
    def _loop(tasks: List[MonitorTask], done: threading.Event):
        while True:
            for task in tasks:
                if os.path.exists(task.signal_path):
                    try:
                        task.execute()
                    except Exception:  # pylint: disable=broad-except
                        logger.exception(
                            "Error running '%s' task, '%s' will be aborted",
                            task.name,
                            task.stage,
                        )
                        Monitor.kill(task.proc)
                        task.killed.set()
                    finally:
                        logger.debug(
                            "Removing signal file for '%s' task", task.name
                        )
                        os.remove(task.signal_path)
            if done.wait(Monitor.AWAIT):
                return
