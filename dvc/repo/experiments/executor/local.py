import logging
import os
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Optional

from funcy import cached_property

from dvc.repo.experiments.base import (
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
)
from dvc.scm import SCM

from .base import BaseExecutor

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.scm.git import Git

logger = logging.getLogger(__name__)


class ExpTemporaryDirectory(TemporaryDirectory):
    # Python's TemporaryDirectory cleanup shutil.rmtree wrapper does not handle
    # git read-only dirs cleanly in Windows on Python <3.8, so we use our own
    # remove(). See:
    # https://github.com/iterative/dvc/pull/5425
    # https://bugs.python.org/issue26660

    @classmethod
    def _rmtree(cls, name):
        from dvc.utils.fs import remove

        remove(name)

    def cleanup(self):
        if self._finalizer.detach():
            self._rmtree(self.name)


class BaseLocalExecutor(BaseExecutor):
    """Base local machine executor."""

    @property
    def git_url(self) -> str:
        root_dir = os.path.abspath(self.root_dir)
        if os.name == "nt":
            root_dir = root_dir.replace(os.sep, "/")
        return f"file://{root_dir}"

    @cached_property
    def scm(self):
        return SCM(self.root_dir)

    def cleanup(self):
        super().cleanup()
        self.scm.close()
        del self.scm


class TempDirExecutor(BaseLocalExecutor):
    """Temp directory experiment executor."""

    # Temp dir executors should warn if untracked files exist (to help with
    # debugging user code), and suppress other DVC hints (like `git add`
    # suggestions) that are not applicable outside of workspace runs
    WARN_UNTRACKED = True
    QUIET = True
    DEFAULT_LOCATION: Optional[str] = "temp"

    def __init__(
        self,
        *args,
        tmp_dir: Optional[str] = None,
        **kwargs,
    ):
        self._tmp_dir = ExpTemporaryDirectory(dir=tmp_dir)
        kwargs["root_dir"] = self._tmp_dir.name
        super().__init__(*args, **kwargs)
        logger.debug("Init temp dir executor in dir '%s'", self._tmp_dir)

    def _init_git(self, scm: "Git", branch: Optional[str] = None, **kwargs):
        from dulwich.repo import Repo as DulwichRepo

        DulwichRepo.init(os.fspath(self.root_dir))

        refspec = f"{EXEC_NAMESPACE}/"
        scm.push_refspec(self.git_url, refspec, refspec, **kwargs)
        if branch:
            scm.push_refspec(self.git_url, branch, branch, **kwargs)
            self.scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
        elif self.scm.get_ref(EXEC_BRANCH):
            self.scm.remove_ref(EXEC_BRANCH)

        if self.scm.get_ref(EXEC_CHECKPOINT):
            self.scm.remove_ref(EXEC_CHECKPOINT)

        # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
        # committing
        head = EXEC_BRANCH if branch else EXEC_HEAD
        self.scm.checkout(head, detach=True)
        merge_rev = self.scm.get_ref(EXEC_MERGE)
        self.scm.merge(merge_rev, squash=True, commit=False)

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w", encoding="utf-8") as fobj:
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    def init_cache(self, dvc: "Repo", rev: str, run_cache: bool = True):
        """Initialize DVC (cache)."""
        self._config(dvc.odb.local.cache_dir)

    def cleanup(self):
        super().cleanup()
        logger.debug("Removing tmpdir '%s'", self._tmp_dir)
        self._tmp_dir.cleanup()
