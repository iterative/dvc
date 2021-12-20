import logging
import posixpath
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Dict, Generator, Optional, Tuple

from ...base import ExpStashEntry
from ..base import BaseExecutor
from ..ssh import SSHExecutor, _sshfs
from .base import BaseExecutorManager

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class SSHExecutorManager(BaseExecutorManager):
    EXECUTOR_CLS = SSHExecutor

    def __init__(
        self,
        scm: "Git",
        wdir: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        fs_factory: Optional[Callable] = None,
        **kwargs,
    ):
        assert host
        super().__init__(scm, wdir, **kwargs)
        self.host = host
        self.port = port
        self.username = username
        self._fs_factory = fs_factory

    def _load_infos(self) -> Generator[Tuple[str, "BaseExecutor"], None, None]:
        # TODO: load existing infos using sshfs
        yield from []

    @classmethod
    def from_stash_entries(
        cls,
        scm: "Git",
        wdir: str,
        repo: "Repo",
        to_run: Dict[str, ExpStashEntry],
        **kwargs,
    ):
        machine_name: Optional[str] = kwargs.get("machine_name", None)
        manager = cls(
            scm, wdir, **repo.machine.get_executor_kwargs(machine_name)
        )
        manager._enqueue_stash_entries(scm, repo, to_run, **kwargs)
        return manager

    def sshfs(self):
        return _sshfs(self._fs_factory, host=self.host, port=self.port)

    def get_infofile_path(self, name: str) -> str:
        return f"{name}{BaseExecutor.INFOFILE_EXT}"

    def _exec_attached(self, repo: "Repo", jobs: Optional[int] = 1):
        from dvc.exceptions import DvcException
        from dvc.stage.monitor import CheckpointKilledError

        assert len(self._queue) == 1
        result: Dict[str, Dict[str, str]] = defaultdict(dict)
        rev, executor = self._queue.popleft()
        info = executor.info
        infofile = posixpath.join(
            info.root_dir,
            info.dvc_dir,
            "tmp",
            self.get_infofile_path(rev),
        )
        try:
            exec_result = executor.reproduce(
                info=executor.info,
                rev=rev,
                infofile=infofile,
                log_level=logger.getEffectiveLevel(),
                fs_factory=self._fs_factory,
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
            self.cleanup_executor(rev, executor)
        return result

    def cleanup_executor(self, rev: str, executor: "BaseExecutor"):
        executor.cleanup()
