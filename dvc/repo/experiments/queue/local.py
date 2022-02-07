import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Mapping

from funcy import cached_property, first

from dvc.exceptions import DvcException

from ..exceptions import ExpQueueEmptyError
from ..executor.base import EXEC_TMP_DIR, BaseExecutor
from ..executor.local import WorkspaceExecutor
from ..refs import EXEC_BRANCH
from .base import BaseStashQueue, QueueEntry, QueueGetResult
from .tasks import setup_exp

if TYPE_CHECKING:
    from celery import Celery

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
    def celery(self) -> "Celery":
        from celery import Celery

        from .celery import get_config

        app = Celery()
        app.conf.update(get_config(self.wdir))
        return app

    @cached_property
    def worker(self) -> "TemporaryWorker":
        from dvc_task.worker import TemporaryWorker

        return TemporaryWorker(self.celery, concurrency=1)

    def put(self, *args, **kwargs) -> QueueEntry:
        """Stash an experiment and add it to the queue."""
        from celery import chain

        entry = super()._stash_exp(*args, **kwargs)
        # schedule executor setup
        # TODO: chain separated git/dvc setup tasks
        chain(
            self.celery.tasks[setup_exp.name](entry.asdict()),
        ).delay()
        return entry

    # NOTE: Queue consumption should not be done directly. Celery worker(s)
    # will automatically consume available experiments.
    def get(self) -> QueueGetResult:
        raise NotImplementedError

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
                    self._collect_executor(executor, exec_result)
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

    def _collect_executor(  # pylint: disable=unused-argument
        self, executor, exec_result
    ) -> Dict[str, str]:
        results = {}
        exp_rev = self.scm.get_ref(EXEC_BRANCH)
        if exp_rev:
            logger.debug("Collected experiment '%s'.", exp_rev[:7])
            results[exp_rev] = exec_result.exp_hash
        return results
