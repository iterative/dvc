import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Generator, Mapping

from funcy import cached_property, first

from dvc.exceptions import DvcException

from ..exceptions import ExpQueueEmptyError
from ..executor.base import EXEC_TMP_DIR, BaseExecutor, ExecutorResult
from ..executor.local import WorkspaceExecutor
from ..refs import EXEC_BRANCH
from .base import BaseStashQueue, QueueEntry, QueueGetResult
from .tasks import setup_exp

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments
    from dvc_task.app import FSApp
    from dvc_task.worker import TemporaryWorker

logger = logging.getLogger(__name__)


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
    def worker(self) -> "TemporaryWorker":
        from dvc_task.worker import TemporaryWorker

        return TemporaryWorker(self.celery, concurrency=1, timeout=10)

    def spawn_worker(self):
        from dvc_task.proc.process import ManagedProcess

        logger.debug("Spawning exp queue worker")
        ManagedProcess.spawn(
            ["dvc", "exp", "queue-worker", "dvc-exp1@localhost"],
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

    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        for msg in self.celery.iter_queued():
            if msg.headers.get("task") != setup_exp.name:
                continue
            args, kwargs, _embed = msg.decode()
            entry_dict = kwargs.get("entry_dict", args[0])
            yield QueueEntry.from_dict(entry_dict)

    def reproduce(self) -> Mapping[str, Mapping[str, str]]:
        raise NotImplementedError


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
