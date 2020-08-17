import logging
import os
import pickle
from tempfile import TemporaryDirectory
from typing import Iterable

from funcy import cached_property

from dvc.path_info import PathInfo
from dvc.repo.tree import RepoTree
from dvc.stage import PipelineStage
from dvc.tree.base import BaseTree
from dvc.tree.local import LocalTree
from dvc.utils import relpath
from dvc.utils.fs import copy_fobj_to_file, makedirs

logger = logging.getLogger(__name__)


class ExperimentExecutor:
    """Base class for executing experiments in parallel.

    Args:
        src_tree: source tree for this experiment.
        baseline_rev: baseline revision that this experiment is derived from.

    Optional keyword args:
        repro_args: Args to be passed into reproduce.
        repro_kwargs: Keyword args to be passed into reproduce.
    """

    def __init__(self, src_tree: BaseTree, baseline_rev: str, **kwargs):
        self.src_tree = src_tree
        self.baseline_rev = baseline_rev
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
        open_func = tree.open if tree else open
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
    """Local machine exepriment executor."""

    def __init__(self, src_tree: BaseTree, baseline_rev: str, **kwargs):
        dvc_dir = kwargs.pop("dvc_dir")
        cache_dir = kwargs.pop("cache_dir")
        super().__init__(src_tree, baseline_rev, **kwargs)
        self.tmp_dir = TemporaryDirectory()
        logger.debug("Init local executor in dir '%s'.", self.tmp_dir)
        self.dvc_dir = os.path.join(self.tmp_dir.name, dvc_dir)
        try:
            for fname in src_tree.walk_files(src_tree.tree_root):
                dest = self.path_info / relpath(fname, src_tree.tree_root)
                if not os.path.exists(dest.parent):
                    makedirs(dest.parent)
                with src_tree.open(fname, "rb") as fobj:
                    copy_fobj_to_file(fobj, dest)
        except Exception:
            self.tmp_dir.cleanup()
            raise
        self._config(cache_dir)
        self._tree = LocalTree(self.dvc, {"url": self.dvc.root_dir})

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

    @staticmethod
    def reproduce(dvc_dir, cwd=None, **kwargs):
        """Run dvc repro and return the result."""
        from dvc.repo import Repo
        from dvc.repo.experiments import hash_exp

        unchanged = []

        def filter_pipeline(stage):
            if isinstance(stage, PipelineStage):
                unchanged.append(stage)

        if cwd:
            old_cwd = os.getcwd()
            os.chdir(cwd)
        else:
            old_cwd = None
            cwd = os.getcwd()

        try:
            logger.debug("Running repro in '%s'", cwd)
            dvc = Repo(dvc_dir)
            dvc.checkout()
            stages = dvc.reproduce(on_unchanged=filter_pipeline, **kwargs)
        finally:
            if old_cwd is not None:
                os.chdir(old_cwd)

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return hash_exp(stages + unchanged)

    def collect_output(self) -> Iterable["PathInfo"]:
        repo_tree = RepoTree(self.dvc)
        for fname in repo_tree.walk_files(repo_tree.root_dir, dvcfiles=True):
            if not repo_tree.isdvc(fname):
                yield self.tree.path_info / fname.relative_to(
                    repo_tree.root_dir
                )

    def cleanup(self):
        logger.debug("Removing tmpdir '%s'", self.tmp_dir)
        self.tmp_dir.cleanup()
        super().cleanup()
