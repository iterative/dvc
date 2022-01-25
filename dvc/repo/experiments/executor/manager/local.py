import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Optional

from ...base import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_HEAD,
    EXEC_MERGE,
    ExpStashEntry,
)
from ..local import TempDirExecutor, WorkspaceExecutor
from .base import BaseExecutorManager

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class TempDirExecutorManager(BaseExecutorManager):
    EXECUTOR_CLS = TempDirExecutor


class WorkspaceExecutorManager(BaseExecutorManager):
    EXECUTOR_CLS = WorkspaceExecutor

    @classmethod
    def from_stash_entries(
        cls,
        scm: "Git",
        wdir: str,
        repo: "Repo",
        to_run: Dict[str, ExpStashEntry],
        **kwargs,
    ):
        manager = cls(scm, wdir)
        try:
            assert len(to_run) == 1
            for stash_rev, entry in to_run.items():
                scm.set_ref(EXEC_HEAD, entry.head_rev)
                scm.set_ref(EXEC_MERGE, stash_rev)
                scm.set_ref(EXEC_BASELINE, entry.baseline_rev)

                executor = cls.EXECUTOR_CLS.from_stash_entry(
                    repo,
                    stash_rev,
                    entry,
                    **kwargs,
                )
                manager.enqueue(stash_rev, executor)
        finally:
            for ref in (EXEC_MERGE,):
                scm.remove_ref(ref)
        return manager

    def _collect_executor(self, repo, executor, exec_result) -> Dict[str, str]:
        results = {}
        exp_rev = self.scm.get_ref(EXEC_BRANCH)
        if exp_rev:
            logger.debug("Collected experiment '%s'.", exp_rev[:7])
            results[exp_rev] = exec_result.exp_hash
        return results

    def exec_queue(
        self, repo: "Repo", jobs: Optional[int] = 1, detach: bool = False
    ):
        """Run a single WorkspaceExecutor.

        Workspace execution is done within the main DVC process
        (rather than in multiprocessing context)
        """
        from dvc.exceptions import DvcException
        from dvc.stage.monitor import CheckpointKilledError

        assert len(self._queue) == 1
        assert not detach
        result: Dict[str, Dict[str, str]] = defaultdict(dict)
        rev, executor = self._queue.popleft()

        exec_name = "workspace"
        infofile = self.get_infofile_path(exec_name)
        try:
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
                result[rev].update(
                    self._collect_executor(repo, executor, exec_result)
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
            self.cleanup_executor(exec_name, executor)
        return result
