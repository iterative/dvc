import functools
import logging
import os
import subprocess
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List

from dvc.stage.decorators import relock_repo
from dvc.stage.exceptions import StageCmdFailedError
from dvc.version import __version__

if TYPE_CHECKING:
    from dvc.stage import Stage


logger = logging.getLogger(__name__)


class CheckpointKilledError(StageCmdFailedError):
    pass


class LiveStudioKilledError(StageCmdFailedError):
    pass


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
    def error_cls(self) -> type:
        raise NotImplementedError

    def is_ready(self):
        pass

    def on_start(self):
        pass

    def on_call_end(self):
        pass

    def on_end(self):
        pass


class LiveStudioTask(MonitorTask):
    name = "live_studio"
    error_cls = LiveStudioKilledError

    def __init__(self, stage: "Stage", proc: subprocess.Popen, url: str):
        super().__init__(stage, self._send_updates, proc)
        import requests
        from requests.adapters import HTTPAdapter, Retry

        self.fs = stage.repo.fs
        self.url = url
        self.rev = stage.repo.scm.get_rev()

        self.session = requests.Session()
        self.session.headers.update({"content-type": "application/json"})
        retries = Retry(
            total=5,
            # TODO: Is this enough for 429 errors?
            backoff_factor=0.2,
            # TODO: Which status to retry?
            # 413 should Not be retried
            status_forcelist=[429, 500, 502, 503, 504],
        )

        self.session.mount(
            "https://studio.iterative.ai/", HTTPAdapter(max_retries=retries)
        )

        self.metrics_status, self.plots_status = self._collect_data()
        self._mtime_metrics = {x: 0 for x in self.metrics_status}
        self._mtime_plots = {x: 0 for x in self.plots_status}

        self._last_plots_step = {x: -1 for x in self.plots_status}

    def on_start(self):
        self._send_updates()

    def is_ready(self):
        self._update_status()
        return any(self.metrics_status.values()) or any(
            self.plots_status.values()
        )

    def _collect_data(self):
        metrics = {}
        plots = {}
        for out in self.stage.outs:
            if out.is_metric:
                metrics[out.def_path] = False
            # Filter out non-linear plots and dirs, for MVP.
            elif out.is_plot and not out.isdir():
                plot_props = {} if isinstance(out.plot, bool) else out.plot
                if plot_props.get("template", "linear") == "linear":
                    plots[out.def_path] = False
        return metrics, plots

    def _update_status(self):
        def _update_mtime(item, container):
            if os.path.exists(item):
                mtime = os.stat(item).st_mtime
                if mtime != container[item]:
                    container[item] = mtime
                    return True
            return False

        for metric in self.metrics_status:
            self.metrics_status[metric] = _update_mtime(
                metric, self._mtime_metrics
            )

        for plot in self.plots_status:
            self.plots_status[plot] = _update_mtime(plot, self._mtime_plots)

    def _get_updates(self):
        from dvc.repo.metrics.show import _read_metric
        from dvc.repo.plots import parse

        updates = {
            # TODO: Is version enough?
            # Should we include some more specific APPEND/REPLACE distinction?
            "version": __version__,
            "rev": self.rev,
            "metrics": {},
            "plots": {},
        }
        for metric, updated in self.metrics_status.items():
            if updated:
                updates["metrics"][metric] = _read_metric(
                    metric, self.fs, self.rev
                )["data"]
        for plot, updated in self.plots_status.items():
            if updated:
                full_plot = parse(self.fs, plot)

                if not full_plot.get("data", {}):
                    continue

                # TODO: MVP assumption, we only send incremental updates
                # for linear plots, so we can rely on `step`.
                new_datapoints = [
                    dict(x)
                    for x in full_plot["data"]
                    if int(x["step"]) > self._last_plots_step[plot]
                ]
                if not new_datapoints:
                    continue
                updates["plots"][plot] = new_datapoints

                self._last_plots_step[plot] = max(
                    int(x["step"]) for x in new_datapoints
                )

        return updates

    def _send_updates(self):
        self.session.post(self.url, json=self._get_updates())


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

    @property
    def signal_path(self) -> str:
        return os.path.join(self.stage.repo.tmp_dir, self.SIGNAL_FILE)

    def is_ready(self):
        return os.path.exists(self.signal_path)

    def on_call_end(self):
        logger.debug("Removing signal file for '%s' task", self.name)
        os.remove(self.signal_path)

    @staticmethod
    @relock_repo
    def _run_callback(stage, callback_func):
        stage.save(allow_missing=True)
        stage.commit(allow_missing=True)
        stage.unprotect_outs()
        logger.debug("Running checkpoint callback for stage '%s'", stage)
        callback_func()


class Monitor:
    AWAIT: float = 1.0

    def __init__(self, tasks: List[MonitorTask]):
        self.done = threading.Event()
        self.tasks = tasks
        self.monitor_thread = threading.Thread(
            target=Monitor._loop, args=(self.tasks, self.done)
        )

    def __enter__(self):
        for t in self.tasks:
            t.on_start()
        self.monitor_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.done.set()
        self.monitor_thread.join()
        for t in self.tasks:
            t.on_end()

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
                if task.is_ready():
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
                        task.on_call_end()
            if done.wait(Monitor.AWAIT):
                return
