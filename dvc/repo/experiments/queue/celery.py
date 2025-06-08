import hashlib
import locale
import logging
import os
from collections import defaultdict
from collections.abc import Collection, Generator, Mapping
from typing import TYPE_CHECKING, NamedTuple, Optional, Union

from celery.result import AsyncResult
from funcy import first

from dvc.daemon import daemonize
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.repo.experiments.exceptions import (
    UnresolvedQueueExpNamesError,
    UnresolvedRunningExpNamesError,
)
from dvc.repo.experiments.executor.base import ExecutorInfo
from dvc.repo.experiments.refs import CELERY_STASH
from dvc.repo.experiments.utils import EXEC_TMP_DIR, get_exp_rwlock
from dvc.ui import ui
from dvc.utils.objects import cached_property

from .base import BaseStashQueue, ExpRefAndQueueEntry, QueueDoneResult, QueueEntry
from .exceptions import CannotKillTasksError
from .tasks import run_exp
from .utils import fetch_running_exp_from_temp_dir

if TYPE_CHECKING:
    from kombu.message import Message

    from dvc.repo.experiments.executor.base import ExecutorResult
    from dvc.repo.experiments.refs import ExpRefInfo
    from dvc.repo.experiments.serialize import ExpExecutor, ExpRange
    from dvc_task.app import FSApp
    from dvc_task.proc.manager import ProcessManager
    from dvc_task.worker import TemporaryWorker

    from .base import QueueGetResult

logger = logger.getChild(__name__)


class _MessageEntry(NamedTuple):
    msg: "Message"
    entry: QueueEntry


class _TaskEntry(NamedTuple):
    async_result: AsyncResult
    entry: QueueEntry


class LocalCeleryQueue(BaseStashQueue):
    """DVC experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    CELERY_DIR = "celery"

    @cached_property
    def wdir(self) -> str:
        assert self.repo.tmp_dir is not None
        return os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, self.CELERY_DIR)

    @cached_property
    def celery(self) -> "FSApp":
        from kombu.transport.filesystem import Channel

        # related to https://github.com/iterative/dvc-task/issues/61
        Channel.QoS.restore_at_shutdown = False

        from dvc_task.app import FSApp

        app = FSApp(
            "dvc-exp-local",
            wdir=self.wdir,
            mkdir=True,
            include=["dvc.repo.experiments.queue.tasks", "dvc_task.proc.tasks"],
        )
        app.conf.update({"task_acks_late": True, "result_expires": None})
        return app

    @cached_property
    def proc(self) -> "ProcessManager":
        from dvc_task.proc.manager import ProcessManager

        return ProcessManager(self.pid_dir)

    @cached_property
    def worker(self) -> "TemporaryWorker":
        from dvc_task.worker import TemporaryWorker

        # NOTE: Use thread pool with concurrency 1 and disabled prefetch.
        # Worker scaling should be handled by running additional workers,
        # rather than increasing pool concurrency.
        #
        # We use "threads" over "solo" (inline single-threaded) execution so
        # that we still have access to the control/broadcast API (which
        # requires a separate message handling thread in the worker).
        #
        # Disabled prefetch ensures that each worker will can only schedule and
        # execute up to one experiment at a time (and a worker cannot prefetch
        # additional experiments from the queue).
        return TemporaryWorker(
            self.celery,
            pool="threads",
            concurrency=1,
            prefetch_multiplier=1,
            without_heartbeat=True,
            without_mingle=True,
            without_gossip=True,
            timeout=10,
            loglevel="debug" if logger.getEffectiveLevel() <= logging.DEBUG else "info",
        )

    def _spawn_worker(self, num: int = 1):
        """spawn one single worker to process to queued tasks.

        Argument:
            num: serial number of the worker.

        """
        from dvc_task.proc.process import ManagedProcess

        logger.debug("Spawning exp queue worker")
        wdir_hash = hashlib.sha256(self.wdir.encode("utf-8")).hexdigest()[:6]
        node_name = f"dvc-exp-{wdir_hash}-{num}@localhost"
        cmd = ["exp", "queue-worker", node_name]
        if num == 1:
            # automatically run celery cleanup when primary worker shuts down
            cmd.append("--clean")
        if logger.getEffectiveLevel() <= logging.DEBUG:
            cmd.append("-v")
        name = f"dvc-exp-worker-{num}"

        logger.debug("start a new worker: %s, node: %s", name, node_name)
        if os.name == "nt":
            daemonize(cmd)
        else:
            ManagedProcess.spawn(["dvc", *cmd], wdir=self.wdir, name=name)

    def start_workers(self, count: int) -> int:
        """start some workers to process the queued tasks.

        Argument:
            count: worker number to be started.

        Returns:
            newly spawned worker number.
        """

        logger.debug("Spawning %s exp queue workers", count)
        active_worker: dict = self.worker_status()

        started = 0
        for num in range(1, 1 + count):
            wdir_hash = hashlib.sha256(self.wdir.encode("utf-8")).hexdigest()[:6]
            node_name = f"dvc-exp-{wdir_hash}-{num}@localhost"
            if node_name in active_worker:
                logger.debug("Exp queue worker %s already exist", node_name)
                continue
            self._spawn_worker(num)
            started += 1

        return started

    def put(
        self,
        *args,
        copy_paths: Optional[list[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ) -> QueueEntry:
        """Stash an experiment and add it to the queue."""
        with get_exp_rwlock(self.repo, writes=["workspace", CELERY_STASH]):
            entry = self._stash_exp(*args, **kwargs)
        self.celery.signature(
            run_exp.s(entry.asdict(), copy_paths=copy_paths, message=message)
        ).delay()
        return entry

    # NOTE: Queue consumption should not be done directly. Celery worker(s)
    # will automatically consume available experiments.
    def get(self) -> "QueueGetResult":
        raise NotImplementedError

    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        for _, entry in self._iter_queued():
            yield entry

    def _iter_queued(self) -> Generator[_MessageEntry, None, None]:
        for msg in self.celery.iter_queued():
            if msg.headers.get("task") != run_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            logger.trace("Found queued task %s", entry_dict["stash_rev"])
            yield _MessageEntry(msg, QueueEntry.from_dict(entry_dict))

    def _iter_processed(self) -> Generator[_MessageEntry, None, None]:
        for msg in self.celery.iter_processed():
            if msg.headers.get("task") != run_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            yield _MessageEntry(msg, QueueEntry.from_dict(entry_dict))

    def _iter_active_tasks(self) -> Generator[_TaskEntry, None, None]:
        for msg, entry in self._iter_processed():
            task_id = msg.headers["id"]
            result: AsyncResult = AsyncResult(task_id)
            if not result.ready():
                logger.trace("Found active task %s", entry.stash_rev)
                yield _TaskEntry(result, entry)

    def _iter_done_tasks(self) -> Generator[_TaskEntry, None, None]:
        for msg, entry in self._iter_processed():
            task_id = msg.headers["id"]
            result: AsyncResult = AsyncResult(task_id)
            if result.ready():
                logger.trace("Found done task %s", entry.stash_rev)
                yield _TaskEntry(result, entry)

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        for _, entry in self._iter_active_tasks():
            yield entry

    def iter_done(self) -> Generator[QueueDoneResult, None, None]:
        for result, entry in self._iter_done_tasks():
            try:
                exp_result = self.get_result(entry)
            except FileNotFoundError:
                if result.status == "SUCCESS":
                    raise DvcException(  # noqa: B904
                        f"Invalid experiment '{entry.stash_rev[:7]}'."
                    )
                if result.status == "FAILURE":
                    exp_result = None
            yield QueueDoneResult(entry, exp_result)

    def iter_success(self) -> Generator[QueueDoneResult, None, None]:
        for queue_entry, exp_result in self.iter_done():
            if exp_result and exp_result.exp_hash and exp_result.ref_info:
                yield QueueDoneResult(queue_entry, exp_result)

    def iter_failed(self) -> Generator[QueueDoneResult, None, None]:
        for queue_entry, exp_result in self.iter_done():
            if exp_result is None:
                yield QueueDoneResult(queue_entry, exp_result)

    def reproduce(
        self, copy_paths: Optional[list[str]] = None, message: Optional[str] = None
    ) -> Mapping[str, Mapping[str, str]]:
        raise NotImplementedError

    def _load_info(self, rev: str) -> ExecutorInfo:
        infofile = self.get_infofile_path(rev)
        return ExecutorInfo.load_json(infofile)

    def _get_done_result(
        self, entry: QueueEntry, timeout: Optional[float] = None
    ) -> Optional["ExecutorResult"]:
        from celery.exceptions import TimeoutError as _CeleryTimeout

        for msg, processed_entry in self._iter_processed():
            if entry.stash_rev == processed_entry.stash_rev:
                task_id = msg.headers["id"]
                result: AsyncResult = AsyncResult(task_id)
                if not result.ready():
                    logger.debug("Waiting for exp task '%s' to complete", result.id)
                    try:
                        result.get(timeout=timeout)
                    except _CeleryTimeout as exc:
                        raise DvcException(
                            "Timed out waiting for exp to finish."
                        ) from exc
                executor_info = self._load_info(entry.stash_rev)
                return executor_info.result
        raise FileNotFoundError

    def get_result(
        self, entry: QueueEntry, timeout: Optional[float] = None
    ) -> Optional["ExecutorResult"]:
        try:
            return self._get_done_result(entry, timeout)
        except FileNotFoundError:
            pass

        for queue_entry in self.iter_queued():
            if entry.stash_rev == queue_entry.stash_rev:
                raise DvcException("Experiment has not been started.")

        # NOTE: It's possible for an exp to complete while iterating through
        # other queued and active tasks, in which case the exp will get moved
        # out of the active task list, and needs to be loaded here.
        return self._get_done_result(entry, timeout)

    def wait(self, revs: Collection[str], **kwargs) -> None:
        """Block until the specified tasks have completed."""
        revs = [revs] if isinstance(revs, str) else revs
        results = self.match_queue_entry_by_name(
            revs, self.iter_queued(), self.iter_done(), self.iter_failed()
        )
        for entry in results.values():
            if not entry:
                continue
            self.wait_for_start(entry, **kwargs)
            try:
                self.get_result(entry)
            except FileNotFoundError:
                pass

    def wait_for_start(self, entry: QueueEntry, sleep_interval: float = 0.001) -> None:
        """Block until the specified task has been started."""
        import time

        while not self.proc.get(entry.stash_rev):
            time.sleep(sleep_interval)

    def _get_running_task_ids(self) -> set[str]:
        running_task_ids: set[str] = set()
        active_workers = self.worker_status()
        for tasks in active_workers.values():
            task = first(tasks)
            if task:
                running_task_ids.add(task["id"])
        return running_task_ids

    def _try_to_kill_tasks(
        self, to_kill: dict[QueueEntry, str], force: bool
    ) -> dict[QueueEntry, str]:
        fail_to_kill_entries: dict[QueueEntry, str] = {}
        for queue_entry, rev in to_kill.items():
            try:
                if force:
                    self.proc.kill(queue_entry.stash_rev)
                else:
                    self.proc.interrupt(queue_entry.stash_rev)
                ui.write(f"{rev} has been killed.")
            except ProcessLookupError:
                fail_to_kill_entries[queue_entry] = rev
        return fail_to_kill_entries

    def _mark_inactive_tasks_failure(
        self, remained_entries: dict[QueueEntry, str]
    ) -> None:
        remained_revs: list[str] = []
        running_ids = self._get_running_task_ids()
        logger.debug("Current running tasks ids: %s.", running_ids)
        for msg, entry in self._iter_processed():
            if entry not in remained_entries:
                continue
            task_id = msg.headers["id"]
            if task_id in running_ids:
                remained_revs.append(remained_entries[entry])
            else:
                result: AsyncResult = AsyncResult(task_id)
                if not result.ready():
                    logger.debug(
                        "Task id %s rev %s marked as failure.",
                        task_id,
                        remained_entries[entry],
                    )
                    backend = self.celery.backend
                    backend.mark_as_failure(task_id, None)  # type: ignore[attr-defined]

        if remained_revs:
            raise CannotKillTasksError(remained_revs)

    def _kill_entries(self, entries: dict[QueueEntry, str], force: bool) -> None:
        logger.debug("Found active tasks: '%s' to kill", list(entries.values()))
        inactive_entries: dict[QueueEntry, str] = self._try_to_kill_tasks(
            entries, force
        )

        if inactive_entries:
            self._mark_inactive_tasks_failure(inactive_entries)

    def kill(self, revs: Collection[str], force: bool = False) -> None:
        name_dict: dict[str, Optional[QueueEntry]] = self.match_queue_entry_by_name(
            set(revs), self.iter_active()
        )

        missing_revs: list[str] = []
        to_kill: dict[QueueEntry, str] = {}
        for rev, queue_entry in name_dict.items():
            if queue_entry is None:
                missing_revs.append(rev)
            else:
                to_kill[queue_entry] = rev

        if to_kill:
            self._kill_entries(to_kill, force)

        if missing_revs:
            raise UnresolvedRunningExpNamesError(missing_revs)

    def shutdown(self, kill: bool = False):
        self.celery.control.shutdown()
        if kill:
            to_kill: dict[QueueEntry, str] = {}
            for entry in self.iter_active():
                to_kill[entry] = entry.name or entry.stash_rev
            if to_kill:
                self._kill_entries(to_kill, True)

    def follow(self, entry: QueueEntry, encoding: Optional[str] = None):
        for line in self.proc.follow(entry.stash_rev, encoding):
            ui.write(line, end="")

    def logs(self, rev: str, encoding: Optional[str] = None, follow: bool = False):
        queue_entry: Optional[QueueEntry] = self.match_queue_entry_by_name(
            {rev}, self.iter_active(), self.iter_done()
        ).get(rev)
        if queue_entry is None:
            if self.match_queue_entry_by_name({rev}, self.iter_queued()).get(rev):
                raise DvcException(
                    f"Experiment '{rev}' is in queue but has not been started"
                )
            raise UnresolvedQueueExpNamesError([rev])
        if follow:
            ui.write(
                f"Following logs for experiment '{rev}'. Use Ctrl+C to stop "
                "following logs (experiment execution will continue).\n"
            )
            try:
                self.follow(queue_entry)
            except KeyboardInterrupt:
                pass
            return
        try:
            proc_info = self.proc[queue_entry.stash_rev]
        except KeyError:
            raise DvcException(  # noqa: B904
                f"No output logs found for experiment '{rev}'"
            )
        with open(
            proc_info.stdout, encoding=encoding or locale.getpreferredencoding()
        ) as fobj:
            ui.write(fobj.read())

    def worker_status(self) -> dict[str, list[dict]]:
        """Return the current active celery worker"""
        status = self.celery.control.inspect().active() or {}
        logger.debug("Worker status: %s", status)
        return status

    def clear(self, *args, **kwargs):
        from .remove import celery_clear

        return celery_clear(self, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from .remove import celery_remove

        return celery_remove(self, *args, **kwargs)

    def get_ref_and_entry_by_names(
        self,
        exp_names: Union[str, list[str]],
        git_remote: Optional[str] = None,
    ) -> dict[str, ExpRefAndQueueEntry]:
        """Find finished ExpRefInfo or queued or failed QueueEntry by name"""
        from dvc.repo.experiments.utils import resolve_name

        if isinstance(exp_names, str):
            exp_names = [exp_names]
        results: dict[str, ExpRefAndQueueEntry] = {}

        exp_ref_match: dict[str, Optional[ExpRefInfo]] = resolve_name(
            self.scm, exp_names, git_remote
        )
        if not git_remote:
            queue_entry_match: dict[str, Optional[QueueEntry]] = (
                self.match_queue_entry_by_name(
                    exp_names, self.iter_queued(), self.iter_done()
                )
            )

        for exp_name in exp_names:
            exp_ref = exp_ref_match[exp_name]
            queue_entry = None if git_remote else queue_entry_match[exp_name]
            results[exp_name] = ExpRefAndQueueEntry(exp_ref, queue_entry)
        return results

    def collect_active_data(
        self,
        baseline_revs: Optional[Collection[str]],
        fetch_refs: bool = False,
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        from dvc.repo import Repo
        from dvc.repo.experiments.collect import collect_exec_branch
        from dvc.repo.experiments.serialize import (
            ExpExecutor,
            ExpRange,
            LocalExpExecutor,
        )

        result: dict[str, list[ExpRange]] = defaultdict(list)
        for entry in self.iter_active():
            if baseline_revs and entry.baseline_rev not in baseline_revs:
                continue
            if fetch_refs:
                fetch_running_exp_from_temp_dir(self, entry.stash_rev, fetch_refs)
            proc_info = self.proc.get(entry.stash_rev)
            executor_info = self._load_info(entry.stash_rev)
            if proc_info:
                local_exec: Optional[LocalExpExecutor] = LocalExpExecutor(
                    root=executor_info.root_dir,
                    log=proc_info.stdout,
                    pid=proc_info.pid,
                    task_id=entry.stash_rev,
                )
            else:
                local_exec = None
            dvc_root = os.path.join(executor_info.root_dir, executor_info.dvc_dir)
            with Repo(dvc_root) as exec_repo:
                kwargs["cache"] = self.repo.experiments.cache
                exps = list(
                    collect_exec_branch(exec_repo, executor_info.baseline_rev, **kwargs)
                )
            exps[0].rev = entry.stash_rev
            exps[0].name = entry.name
            result[entry.baseline_rev].append(
                ExpRange(
                    exps,
                    executor=ExpExecutor(
                        "running",
                        name=executor_info.location,
                        local=local_exec,
                    ),
                    name=entry.name,
                )
            )
        return result

    def collect_queued_data(
        self, baseline_revs: Optional[Collection[str]], **kwargs
    ) -> dict[str, list["ExpRange"]]:
        from dvc.repo.experiments.collect import collect_rev
        from dvc.repo.experiments.serialize import (
            ExpExecutor,
            ExpRange,
            LocalExpExecutor,
        )

        result: dict[str, list[ExpRange]] = defaultdict(list)
        for entry in self.iter_queued():
            if baseline_revs and entry.baseline_rev not in baseline_revs:
                continue
            exp = collect_rev(self.repo, entry.stash_rev, **kwargs)
            exp.name = entry.name
            local_exec: Optional[LocalExpExecutor] = LocalExpExecutor(
                task_id=entry.stash_rev,
            )
            result[entry.baseline_rev].append(
                ExpRange(
                    [exp],
                    executor=ExpExecutor("queued", name="dvc-task", local=local_exec),
                    name=entry.name,
                )
            )
        return result

    def collect_failed_data(
        self,
        baseline_revs: Optional[Collection[str]],
        **kwargs,
    ) -> dict[str, list["ExpRange"]]:
        from dvc.repo.experiments.collect import collect_rev
        from dvc.repo.experiments.serialize import (
            ExpExecutor,
            ExpRange,
            LocalExpExecutor,
            SerializableError,
        )

        result: dict[str, list[ExpRange]] = defaultdict(list)
        for entry, _ in self.iter_failed():
            if baseline_revs and entry.baseline_rev not in baseline_revs:
                continue
            proc_info = self.proc.get(entry.stash_rev)
            if proc_info:
                local_exec: Optional[LocalExpExecutor] = LocalExpExecutor(
                    log=proc_info.stdout,
                    pid=proc_info.pid,
                    returncode=proc_info.returncode,
                    task_id=entry.stash_rev,
                )
            else:
                local_exec = None
            exp = collect_rev(self.repo, entry.stash_rev, **kwargs)
            exp.name = entry.name
            exp.error = SerializableError("Experiment run failed")
            result[entry.baseline_rev].append(
                ExpRange(
                    [exp],
                    executor=ExpExecutor("failed", local=local_exec),
                    name=entry.name,
                )
            )
        return result

    def collect_success_executors(
        self,
        baseline_revs: Optional[Collection[str]],
        **kwargs,
    ) -> dict[str, "ExpExecutor"]:
        """Map exp refs to any available successful executors."""
        from dvc.repo.experiments.serialize import ExpExecutor, LocalExpExecutor

        result: dict[str, ExpExecutor] = {}
        for entry, exec_result in self.iter_success():
            if baseline_revs and entry.baseline_rev not in baseline_revs:
                continue
            if not (exec_result and exec_result.ref_info):
                continue
            proc_info = self.proc.get(entry.stash_rev)
            if proc_info:
                local_exec: Optional[LocalExpExecutor] = LocalExpExecutor(
                    log=proc_info.stdout,
                    pid=proc_info.pid,
                    returncode=proc_info.returncode,
                    task_id=entry.stash_rev,
                )
            else:
                local_exec = None
            result[str(exec_result.ref_info)] = ExpExecutor(
                "success", name="dvc-task", local=local_exec
            )
        return result
