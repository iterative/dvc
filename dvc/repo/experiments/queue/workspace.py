import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING, Collection, Dict, Generator, Optional

from funcy import first

from dvc.exceptions import DvcException
from dvc.utils.fs import remove

from ..exceptions import ExpQueueEmptyError
from ..executor.base import BaseExecutor, ExecutorResult
from ..executor.local import WorkspaceExecutor
from ..refs import EXEC_BRANCH
from .base import BaseStashQueue, QueueDoneResult, QueueEntry, QueueGetResult

if TYPE_CHECKING:
    from dvc.repo.experiments import Experiments

logger = logging.getLogger(__name__)


class WorkspaceQueue(BaseStashQueue):
    _EXEC_NAME: Optional[str] = "workspace"

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
            stash_entry.head_rev,
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
                entry.head_rev,
            )

    def iter_active(self) -> Generator[QueueEntry, None, None]:
        # Workspace run state is reflected in the workspace itself and does not
        # need to be handled via the queue
        raise NotImplementedError

    def iter_done(self) -> Generator[QueueDoneResult, None, None]:
        raise NotImplementedError

    def iter_failed(self) -> Generator[QueueDoneResult, None, None]:
        raise NotImplementedError

    def iter_success(self) -> Generator[QueueDoneResult, None, None]:
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
        exec_name = self._EXEC_NAME or entry.stash_rev
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
            if self._EXEC_NAME == exec_name:
                remove(os.path.join(self.pid_dir, exec_name))
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

    def kill(self, revs: Collection[str]) -> None:
        raise NotImplementedError

    def shutdown(self, kill: bool = False):
        raise NotImplementedError

    def logs(
        self,
        rev: str,
        encoding: Optional[str] = None,
        follow: bool = False,
    ):
        raise NotImplementedError
