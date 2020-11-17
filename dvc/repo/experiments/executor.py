import logging
import os
import pickle
from tempfile import TemporaryDirectory
from typing import Iterable, Optional

from funcy import cached_property

from dvc.path_info import PathInfo
from dvc.stage import PipelineStage
from dvc.tree.base import BaseTree
from dvc.tree.local import LocalTree
from dvc.tree.repo import RepoTree

logger = logging.getLogger(__name__)


class ExperimentExecutor:
    """Base class for executing experiments in parallel.

    Args:
        baseline_rev: Baseline revision that this experiment is derived from.

    Optional keyword args:
        branch: Existing git branch for this experiment.
        rev: Git revision to be checked out for this experiment, defaults to
            branch, baseline_rev in that order.
        repro_args: Args to be passed into reproduce.
        repro_kwargs: Keyword args to be passed into reproduce.
    """

    def __init__(
        self,
        baseline_rev: str,
        branch: Optional[str] = None,
        rev: Optional[str] = None,
        **kwargs,
    ):
        self.baseline_rev = baseline_rev
        self._rev = rev
        self.branch = branch
        self.repro_args = kwargs.pop("repro_args", [])
        self.repro_kwargs = kwargs.pop("repro_kwargs", {})

    @property
    def tree(self) -> BaseTree:
        raise NotImplementedError

    @staticmethod
    def reproduce(dvc_dir, cwd=None, **kwargs):
        raise NotImplementedError

    def collect_output(self) -> Iterable["PathInfo"]:
        """Iterate over output pathnames for this executor.

        For DVC outs, only the .dvc file path will be yielded. DVC outs
        themselves should be fetched from remote executor cache in the normal
        fetch/pull way. For local executors outs will already be available via
        shared local cache.
        """
        raise NotImplementedError

    def cleanup(self):
        pass

    # TODO: come up with better way to stash repro arguments
    @staticmethod
    def pack_repro_args(path, *args, tree=None, **kwargs):
        dpath = os.path.dirname(path)
        if tree:
            open_func = tree.open
            tree.makedirs(dpath)
        else:
            from dvc.utils.fs import makedirs

            open_func = open
            makedirs(dpath, exist_ok=True)
        data = {"args": args, "kwargs": kwargs}
        with open_func(path, "wb") as fobj:
            pickle.dump(data, fobj)

    @staticmethod
    def unpack_repro_args(path, tree=None):
        open_func = tree.open if tree else open
        with open_func(path, "rb") as fobj:
            data = pickle.load(fobj)
        return data["args"], data["kwargs"]


class LocalExecutor(ExperimentExecutor):
    """Local machine experiment executor."""

    def __init__(
        self, checkpoint_reset: Optional[bool] = False, **kwargs,
    ):
        from dvc.repo import Repo

        dvc_dir = kwargs.pop("dvc_dir")
        cache_dir = kwargs.pop("cache_dir")
        super().__init__(**kwargs)
        self.tmp_dir = TemporaryDirectory()

        # init empty DVC repo (will be overwritten when input is uploaded)
        Repo.init(root_dir=self.tmp_dir.name, no_scm=True)
        logger.debug(
            "Init local executor in dir '%s' with baseline '%s'.",
            self.tmp_dir,
            self.baseline_rev[:7],
        )
        self.dvc_dir = os.path.join(self.tmp_dir.name, dvc_dir)
        self._config(cache_dir)
        self._tree = LocalTree(self.dvc, {"url": self.dvc.root_dir})
        # override default CACHE_MODE since files must be writable in order
        # to run repro
        self._tree.CACHE_MODE = 0o644
        self.checkpoint_reset = checkpoint_reset
        self.checkpoint = False

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write("[core]\n    no_scm = true\n")
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    @cached_property
    def dvc(self):
        from dvc.repo import Repo

        return Repo(self.dvc_dir)

    @cached_property
    def path_info(self):
        return PathInfo(self.tmp_dir.name)

    @property
    def tree(self):
        return self._tree

    @property
    def rev(self):
        return self._rev if self._rev else self.baseline_rev

    @staticmethod
    def reproduce(dvc_dir, cwd=None, **kwargs):
        """Run dvc repro and return the result."""
        from dvc.repo import Repo

        unchanged = []

        def filter_pipeline(stages):
            unchanged.extend(
                [stage for stage in stages if isinstance(stage, PipelineStage)]
            )

        if cwd:
            old_cwd = os.getcwd()
            os.chdir(cwd)
        else:
            old_cwd = None
            cwd = os.getcwd()

        try:
            logger.debug("Running repro in '%s'", cwd)
            dvc = Repo(dvc_dir)

            # NOTE: for checkpoint experiments we handle persist outs slightly
            # differently than normal:
            #
            # - checkpoint out may not yet exist if this is the first time this
            #   experiment has been run, this is not an error condition for
            #   experiments
            # - at the start of a repro run, we need to remove the persist out
            #   and restore it to its last known (committed) state (which may
            #   be removed/does not yet exist) so that our executor workspace
            #   is not polluted with the (persistent) out from an unrelated
            #   experiment run
            dvc.checkout(force=True, quiet=True)
            stages = dvc.reproduce(on_unchanged=filter_pipeline, **kwargs)
        finally:
            if old_cwd is not None:
                os.chdir(old_cwd)

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return stages + unchanged

    def collect_output(self) -> Iterable["PathInfo"]:
        repo_tree = RepoTree(self.dvc)
        yield from self.collect_files(self.tree, repo_tree)

    @staticmethod
    def collect_files(tree: BaseTree, repo_tree: RepoTree):
        for fname in repo_tree.walk_files(repo_tree.root_dir, dvcfiles=True):
            if not repo_tree.isdvc(fname):
                yield tree.path_info / fname.relative_to(repo_tree.root_dir)

    def cleanup(self):
        logger.debug("Removing tmpdir '%s'", self.tmp_dir)
        self.tmp_dir.cleanup()
        super().cleanup()
