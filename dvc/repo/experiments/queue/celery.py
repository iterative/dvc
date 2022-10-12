import hashlib
import locale
import logging
import os
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    Generator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Set,
)

from celery.result import AsyncResult
from funcy import cached_property
from kombu.message import Message

from dvc.daemon import daemonize
from dvc.exceptions import DvcException
from dvc.ui import ui

from ..exceptions import UnresolvedQueueExpNamesError
from ..executor.base import EXEC_TMP_DIR, ExecutorInfo, ExecutorResult
from .base import BaseStashQueue, QueueDoneResult, QueueEntry, QueueGetResult
from .tasks import run_exp
from .utils import fetch_running_exp_from_temp_dir

if TYPE_CHECKING:
    from dvc_task.app import FSApp
    from dvc_task.proc.manager import ProcessManager
    from dvc_task.worker import TemporaryWorker

logger = logging.getLogger(__name__)


class _MessageEntry(NamedTuple):
    msg: Message
    entry: QueueEntry


class _TaskEntry(NamedTuple):
    async_result: AsyncResult
    entry: QueueEntry


class LocalCeleryQueue(BaseStashQueue):
    """DVC experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    CELERY_DIR = "celery"

    _shutdown_task_ids: Set[str] = set()

    @cached_property
    def wdir(self) -> str:
        return os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, self.CELERY_DIR)

    @cached_property
    def celery(self) -> "FSApp":
        from kombu.transport.filesystem import Channel  # type: ignore

        # related to https://github.com/iterative/dvc-task/issues/61
        Channel.QoS.restore_at_shutdown = False

        from dvc_task.app import FSApp

        app = FSApp(
            "dvc-exp-local",
            wdir=self.wdir,
            mkdir=True,
            include=[
                "dvc.repo.experiments.queue.tasks",
                "dvc_task.proc.tasks",
            ],
        )
        app.conf.update({"task_acks_late": True})
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
        )

    def spawn_worker(self, num: int = 1):
        """spawn one single worker to process to queued tasks.

        Argument:
            num: serial number of the worker.

        """
        from dvc_task.proc.process import ManagedProcess

        logger.debug("Spawning exp queue worker")
        wdir_hash = hashlib.sha256(self.wdir.encode("utf-8")).hexdigest()[:6]
        node_name = f"dvc-exp-{wdir_hash}-{num}@localhost"
        cmd = ["exp", "queue-worker", node_name]
        name = f"dvc-exp-worker-{num}"

        logger.debug(f"start a new worker: {name}, node: {node_name}")
        if os.name == "nt":
            daemonize(cmd)
        else:
            ManagedProcess.spawn(["dvc"] + cmd, wdir=self.wdir, name=name)

    def start_workers(self, count: int) -> int:
        """start some workers to process the queued tasks.

        Argument:
            count: worker number to be started.

        Returns:
            newly spawned worker number.
        """

        logger.debug(f"Spawning {count} exp queue workers")
        active_worker: Dict = self.worker_status()

        started = 0
        for num in range(1, 1 + count):
            wdir_hash = hashlib.sha256(self.wdir.encode("utf-8")).hexdigest()[
                :6
            ]
            node_name = f"dvc-exp-{wdir_hash}-{num}@localhost"
            if node_name in active_worker:
                logger.debug(f"Exp queue worker {node_name} already exist")
                continue
            self.spawn_worker(num)
            started += 1

        return started

    def put(self, *args, **kwargs) -> QueueEntry:
        """Stash an experiment and add it to the queue."""
        entry = self._stash_exp(*args, **kwargs)
        self.celery.signature(run_exp.s(entry.asdict())).delay()
        return entry

    # NOTE: Queue consumption should not be done directly. Celery worker(s)
    # will automatically consume available experiments.
    def get(self) -> QueueGetResult:
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
            logger.debug("Found queued task %s", entry_dict["stash_rev"])
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
                logger.debug("Found active task %s", entry.stash_rev)
                yield _TaskEntry(result, entry)

    def _iter_done_tasks(self) -> Generator[_TaskEntry, None, None]:

        for msg, entry in self._iter_processed():
            task_id = msg.headers["id"]
            result: AsyncResult = AsyncResult(task_id)
            if result.ready():
                logger.debug("Found done task %s", entry.stash_rev)
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
                    raise DvcException(
                        f"Invalid experiment '{entry.stash_rev[:7]}'."
                    )
                elif result.status == "FAILURE":
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

    def reproduce(self) -> Mapping[str, Mapping[str, str]]:
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
                    logger.debug(
                        "Waiting for exp task '%s' to complete", result.id
                    )
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

    def kill(self, revs: Collection[str]) -> None:
        to_kill: Set[QueueEntry] = set()
        name_dict: Dict[
            str, Optional[QueueEntry]
        ] = self.match_queue_entry_by_name(set(revs), self.iter_active())

        missing_rev: List[str] = []
        for rev, queue_entry in name_dict.items():
            if queue_entry is None:
                missing_rev.append(rev)
            else:
                to_kill.add(queue_entry)

        if missing_rev:
            raise UnresolvedQueueExpNamesError(missing_rev)

        for queue_entry in to_kill:
            self.proc.kill(queue_entry.stash_rev)

    def shutdown(self, kill: bool = False):
        self.celery.control.shutdown()
        if kill:
            for _, task_entry in self._iter_active_tasks():
                try:
                    self.proc.kill(task_entry.stash_rev)
                except ProcessLookupError:
                    continue

    def follow(
        self,
        entry: QueueEntry,
        encoding: Optional[str] = None,
    ):
        for line in self.proc.follow(entry.stash_rev, encoding):
            ui.write(line, end="")

    def logs(
        self,
        rev: str,
        encoding: Optional[str] = None,
        follow: bool = False,
    ):
        queue_entry: Optional[QueueEntry] = self.match_queue_entry_by_name(
            {rev}, self.iter_active(), self.iter_done()
        ).get(rev)
        if queue_entry is None:
            if rev in self.match_queue_entry_by_name(
                {rev}, self.iter_queued()
            ):
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
            raise DvcException(f"No output logs found for experiment '{rev}'")
        with open(
            proc_info.stdout,
            encoding=encoding or locale.getpreferredencoding(),
        ) as fobj:
            ui.write(fobj.read())

    def worker_status(self) -> Dict:
        """Return the current active celery worker"""
        status = self.celery.control.inspect().active() or {}
        logger.debug(f"Worker status: {status}")
        return status

    def clear(self, *args, **kwargs):
        from .remove import celery_clear

        return celery_clear(self, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from .remove import celery_remove

        return celery_remove(self, *args, **kwargs)

    def get_running_exps(self, fetch_refs: bool = True) -> Dict[str, Dict]:
        """Get the execution info of the currently running experiments

        Args:
            fetch_ref (bool): fetch completed checkpoints or not.
        """
        result: Dict[str, Dict] = {}
        for entry in self.iter_active():
            result.update(
                fetch_running_exp_from_temp_dir(
                    self, entry.stash_rev, fetch_refs
                )
            )
        return result
