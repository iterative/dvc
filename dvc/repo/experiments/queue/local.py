import hashlib
import logging
import os
import time
from collections import defaultdict
from typing import (
    TYPE_CHECKING,
    Dict,
    Generator,
    List,
    Mapping,
    NamedTuple,
    Optional,
)

from funcy import cached_property, first
from kombu.message import Message

from dvc.exceptions import DvcException

from ..exceptions import ExpQueueEmptyError
from ..executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorInfo,
    ExecutorResult,
)
from ..executor.local import WorkspaceExecutor
from ..refs import EXEC_BRANCH
from ..stash import ExpStashEntry
from .base import BaseStashQueue, QueueEntry, QueueGetResult
from .tasks import setup_exp

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc_task.app import FSApp
    from dvc_task.proc.manager import ProcessManager
    from dvc_task.worker import TemporaryWorker

logger = logging.getLogger(__name__)


class _MessageEntry(NamedTuple):
    msg: Message
    entry: QueueEntry


class LocalCeleryQueue(BaseStashQueue):
    """DVC experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    CELERY_DIR = "celery"

    @cached_property
    def wdir(self) -> str:
        return os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, self.CELERY_DIR)

    @cached_property
    def celery(self) -> "FSApp":
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
        return app

    @cached_property
    def proc(self) -> "ProcessManager":
        from dvc_task.proc.manager import ProcessManager

        pid_dir = os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)
        return ProcessManager(pid_dir)

    @cached_property
    def worker(self) -> "TemporaryWorker":
        from dvc_task.worker import TemporaryWorker

        return TemporaryWorker(self.celery, concurrency=1, timeout=10)

    def spawn_worker(self):
        from dvc_task.proc.process import ManagedProcess

        logger.debug("Spawning exp queue worker")
        wdir_hash = hashlib.sha256(self.wdir.encode("utf-8")).hexdigest()[:6]
        node_name = f"dvc-exp-{wdir_hash}-1@localhost"
        ManagedProcess.spawn(
            ["dvc", "exp", "queue-worker", node_name],
            wdir=self.wdir,
            name="dvc-exp-worker",
        )

    def put(self, *args, **kwargs) -> QueueEntry:
        """Stash an experiment and add it to the queue."""
        entry = self._stash_exp(*args, **kwargs)
        # schedule executor setup
        # TODO: chain separated git/dvc setup tasks
        self.celery.tasks[setup_exp.name].delay(entry.asdict())
        return entry

    # NOTE: Queue consumption should not be done directly. Celery worker(s)
    # will automatically consume available experiments.
    def get(self) -> QueueGetResult:
        raise NotImplementedError

    def _remove_revs(self, stash_revs: Mapping[str, ExpStashEntry]):
        to_drop: List[int] = []
        try:
            for msg, queue_entry in self._iter_queued():
                if queue_entry.stash_rev in stash_revs:
                    self.celery.reject(msg.delivery_tag)
                    stash_entry = stash_revs[queue_entry.stash_rev]
                    assert stash_entry.stash_index is not None
                    to_drop.append(stash_entry.stash_index)
        finally:
            for index in sorted(to_drop, reverse=True):
                self.stash.drop(index)

    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        for _, entry in self._iter_queued():
            yield entry

    def _iter_queued(self) -> Generator[_MessageEntry, None, None]:
        for msg in self.celery.iter_queued():
            if msg.headers.get("task") != setup_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            yield _MessageEntry(msg, QueueEntry.from_dict(entry_dict))

    def _iter_processed(self) -> Generator[QueueEntry, None, None]:
        for msg in self.celery.iter_processed():
            if msg.headers.get("task") != setup_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            yield QueueEntry.from_dict(entry_dict)

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        for entry in self._iter_processed():
            proc_info = self.proc.get(entry.stash_rev)
            if proc_info is not None and proc_info.returncode is None:
                yield entry

    def reproduce(self) -> Mapping[str, Mapping[str, str]]:
        raise NotImplementedError

    def get_result(self, entry: QueueEntry) -> Optional[ExecutorResult]:
        infofile = self.get_infofile_path(entry.stash_rev)
        while True:
            try:
                executor_info = ExecutorInfo.load_json(infofile)
                if executor_info.collected:
                    return executor_info.result
            except FileNotFoundError:
                # Infofile will not be created until execution begins
                pass
            time.sleep(1)


class WorkspaceQueue(BaseStashQueue):
    def put(self, *args, **kwargs) -> QueueEntry:
        return self._stash_exp(*args, **kwargs)

    def get(self) -> QueueGetResult:
        revs = self.stash.stash_revs
        if not revs:
            raise ExpQueueEmptyError("No experiments in the queue.")
        stash_rev, stash_entry = first(revs.items())
        entry = QueueEntry(
            self.repo.root_dir,
            self.scm.root_dir,
            self.ref,
            stash_rev,
            stash_entry.baseline_rev,
            stash_entry.branch,
            stash_entry.name,
        )
        executor = self.setup_executor(self.repo.experiments, entry)
        return QueueGetResult(entry, executor)

    def _remove_revs(self, stash_revs: Mapping[str, ExpStashEntry]):
        for index in sorted(
            (
                entry.stash_index
                for entry in stash_revs.values()
                if entry.stash_index is not None
            ),
            reverse=True,
        ):
            self.stash.drop(index)

    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        for rev, entry in self.stash.stash_revs:
            yield QueueEntry(
                self.repo.root_dir,
                self.scm.root_dir,
                self.ref,
                rev,
                entry.baseline_rev,
                entry.branch,
                entry.name,
            )

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        # Workspace run state is reflected in the workspace itself and does not
        # need to be handled via the queue
        raise NotImplementedError

    def reproduce(self) -> Dict[str, Dict[str, str]]:
        results: Dict[str, Dict[str, str]] = defaultdict(dict)
        try:
            while True:
                entry, executor = self.get()
                results.update(self._reproduce_entry(entry, executor))
        except ExpQueueEmptyError:
            pass
        return results

    def _reproduce_entry(
        self, entry: QueueEntry, executor: BaseExecutor
    ) -> Dict[str, Dict[str, str]]:
        from dvc.stage.monitor import CheckpointKilledError

        results: Dict[str, Dict[str, str]] = defaultdict(dict)
        exec_name = "workspace"
        infofile = self.get_infofile_path(exec_name)
        try:
            rev = entry.stash_rev
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
                log_errors=not isinstance(executor, WorkspaceExecutor),
            )
            if not exec_result.exp_hash:
                raise DvcException(
                    f"Failed to reproduce experiment '{rev[:7]}'"
                )
            if exec_result.ref_info:
                results[rev].update(
                    self.collect_executor(
                        self.repo.experiments, executor, exec_result
                    )
                )
        except CheckpointKilledError:
            # Checkpoint errors have already been logged
            return {}
        except DvcException:
            raise
        except Exception as exc:
            raise DvcException(
                f"Failed to reproduce experiment '{rev[:7]}'"
            ) from exc
        finally:
            executor.cleanup()
        return results

    @staticmethod
    def collect_executor(  # pylint: disable=unused-argument
        exp: "Experiments",
        executor: BaseExecutor,
        exec_result: ExecutorResult,
    ) -> Dict[str, str]:
        results: Dict[str, str] = {}
        exp_rev = exp.scm.get_ref(EXEC_BRANCH)
        if exp_rev:
            assert exec_result.exp_hash
            logger.debug("Collected experiment '%s'.", exp_rev[:7])
            results[exp_rev] = exec_result.exp_hash
        return results

    def get_result(self, entry: QueueEntry) -> Optional[ExecutorResult]:
        raise NotImplementedError
