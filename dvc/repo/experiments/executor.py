import logging
import os
import pickle
from functools import partial
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Tuple, Union

from funcy import cached_property

from dvc.dvcfile import is_lock_file
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.repo.experiments.base import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_HEAD,
    EXEC_MERGE,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    ExpRefInfo,
    UnchangedExperimentError,
)
from dvc.scm import SCM
from dvc.scm.git import Git
from dvc.stage import PipelineStage
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256
from dvc.utils.fs import remove

if TYPE_CHECKING:
    from multiprocessing import Queue

logger = logging.getLogger(__name__)


class BaseExecutor:
    """Base class for executing experiments in parallel.

    Args:
        src: source Git SCM instance.
        dvc_dir: relpath to DVC root from SCM root.

    Optional keyword args:
        branch: Existing git branch for this experiment.
    """

    PACKED_ARGS_FILE = "repro.dat"

    def __init__(
        self,
        src: "Git",
        dvc_dir: str,
        root_dir: Optional[Union[str, PathInfo]] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        assert root_dir is not None
        self._dvc_dir = dvc_dir
        self.root_dir = root_dir
        self._init_git(src, branch)
        self.name = name

    def _init_git(self, scm: "Git", branch: Optional[str] = None):
        """Init git repo and collect executor refs from the specified SCM."""
        from dulwich.repo import Repo as DulwichRepo

        DulwichRepo.init(os.fspath(self.root_dir))

        cwd = os.getcwd()
        os.chdir(self.root_dir)
        try:
            refspec = f"{EXEC_NAMESPACE}/"
            scm.push_refspec(self.git_url, refspec, refspec)
            if branch:
                scm.push_refspec(self.git_url, branch, branch)
                self.scm.set_ref(EXEC_BRANCH, branch, symbolic=True)

            if self.scm.get_ref(EXEC_CHECKPOINT):
                self.scm.remove_ref(EXEC_CHECKPOINT)

            # checkout EXEC_HEAD and apply EXEC_MERGE on top of it without
            # committing
            head = EXEC_BRANCH if branch else EXEC_HEAD
            self.scm.checkout(head, detach=True)
            self.scm.repo.git.merge(EXEC_MERGE, squash=True, no_commit=True)
            self.scm.repo.git.reset()
            self._prune_lockfiles()
        finally:
            os.chdir(cwd)

    def _prune_lockfiles(self):
        # NOTE: dirty DVC lock files must be restored to index state to
        # avoid checking out incorrect persist or checkpoint outs
        dirty = [diff.a_path for diff in self.scm.repo.index.diff(None)]
        to_checkout = [fname for fname in dirty if is_lock_file(fname)]
        self.scm.repo.index.checkout(paths=to_checkout, force=True)

        untracked = self.scm.repo.untracked_files
        to_remove = [fname for fname in untracked if is_lock_file(fname)]
        for fname in to_remove:
            remove(fname)
        return (
            len(dirty) - len(to_checkout) + len(untracked) - len(to_remove)
        ) != 0

    @cached_property
    def scm(self):
        return SCM(self.root_dir)

    @property
    def git_url(self) -> str:
        raise NotImplementedError

    @property
    def dvc_dir(self) -> str:
        return os.path.join(self.root_dir, self._dvc_dir)

    @staticmethod
    def hash_exp(stages):
        exp_data = {}
        for stage in stages:
            if isinstance(stage, PipelineStage):
                exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    def cleanup(self):
        self.scm.close()
        del self.scm

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
    def unpack_repro_args(path):
        with open(path, "rb") as fobj:
            data = pickle.load(fobj)
        return data["args"], data["kwargs"]

    def fetch_exps(
        self,
        dest_scm: "Git",
        force: bool = False,
        on_diverged: Callable[[str], None] = None,
    ) -> Iterable[str]:
        """Fetch reproduced experiments into the specified SCM."""
        refs = []
        for ref in self.scm.iter_refs(base=EXPS_NAMESPACE):
            if not ref.startswith(EXEC_NAMESPACE) and ref != EXPS_STASH:
                refs.append(ref)

        def on_diverged_ref(orig_ref, new_rev):
            orig_rev = dest_scm.get_ref(orig_ref)
            if dest_scm.diff(orig_rev, new_rev):
                if force:
                    logger.debug(
                        "Replacing existing experiment '%s'",
                        os.fsdecode(orig_ref),
                    )
                    return True
                if on_diverged:
                    checkpoint = self.scm.get_ref(EXEC_CHECKPOINT) is not None
                    on_diverged(orig_ref, checkpoint)
            logger.debug(
                "Reproduced existing experiment '%s'", os.fsdecode(orig_ref)
            )
            return False

        # fetch experiments
        dest_scm.fetch_refspecs(
            self.git_url,
            [f"{ref}:{ref}" for ref in refs],
            on_diverged=on_diverged_ref,
            force=force,
        )
        # update last run checkpoint (if it exists)
        if self.scm.get_ref(EXEC_CHECKPOINT):
            dest_scm.fetch_refspecs(
                self.git_url,
                [f"{EXEC_CHECKPOINT}:{EXEC_CHECKPOINT}"],
                force=True,
            )
        return refs

    @classmethod
    def reproduce(
        cls,
        dvc_dir: str,
        queue: "Queue",
        rev: str,
        cwd: Optional[str] = None,
        name: Optional[str] = None,
    ) -> Tuple[bool, Optional[str]]:
        """Run dvc repro and return the result.

        Returns tuple of (exp_hash, force) where exp_hash is the experiment
            hash (or None on error) and force is a bool specifying whether or
            not this experiment should force overwrite any existing duplicates.
        """
        unchanged = []

        queue.put((rev, os.getpid()))

        def filter_pipeline(stages):
            unchanged.extend(
                [stage for stage in stages if isinstance(stage, PipelineStage)]
            )

        result = None
        force = False

        try:
            dvc = Repo(dvc_dir)
            old_cwd = os.getcwd()
            new_cwd = cwd if cwd else dvc.root_dir
            os.chdir(new_cwd)
            logger.debug("Running repro in '%s'", cwd)

            args_path = os.path.join(
                dvc.tmp_dir, BaseExecutor.PACKED_ARGS_FILE
            )
            if os.path.exists(args_path):
                args, kwargs = BaseExecutor.unpack_repro_args(args_path)
                remove(args_path)
            else:
                args = []
                kwargs = {}

            force = kwargs.get("force", False)

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

            # We cannot use dvc.scm to make commits inside the executor since
            # cached props are not picklable.
            scm = Git()
            checkpoint_func = partial(cls.checkpoint_callback, scm, name)
            stages = dvc.reproduce(
                *args,
                on_unchanged=filter_pipeline,
                checkpoint_func=checkpoint_func,
                **kwargs,
            )

            exp_hash = cls.hash_exp(stages)
            exp_rev = cls.commit(scm, exp_hash, exp_name=name)
            if scm.get_ref(EXEC_CHECKPOINT):
                scm.set_ref(EXEC_CHECKPOINT, exp_rev)
        except UnchangedExperimentError:
            pass
        finally:
            if scm:
                scm.close()
                del scm
            if old_cwd:
                os.chdir(old_cwd)

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return result, force

    @classmethod
    def checkpoint_callback(
        cls,
        scm: "Git",
        name: Optional[str],
        unchanged: Iterable[PipelineStage],
        stages: Iterable[PipelineStage],
    ):
        try:
            exp_hash = cls.hash_exp(stages + unchanged)
            exp_rev = cls.commit(scm, exp_hash, exp_name=name)
            scm.set_ref(EXEC_CHECKPOINT, exp_rev)
            logger.info("Checkpoint experiment iteration '%s'.", exp_rev[:7])
        except UnchangedExperimentError:
            pass

    @classmethod
    def commit(cls, scm: "Git", exp_hash: str, exp_name: Optional[str] = None):
        """Commit stages as an experiment and return the commit SHA."""
        rev = scm.get_rev()
        if not scm.is_dirty(untracked_files=True):
            logger.debug("No changes to commit")
            raise UnchangedExperimentError(rev)

        branch = scm.get_ref(EXEC_BRANCH, follow=False)
        if branch:
            old_ref = rev
            logger.debug("Commit to current experiment branch '%s'", branch)
        else:
            baseline_rev = scm.get_ref(EXEC_BASELINE)
            name = exp_name if exp_name else f"exp-{exp_hash[:5]}"
            branch = str(ExpRefInfo(baseline_rev, name))
            old_ref = None
            logger.debug("Commit to new experiment branch '%s'", branch)

        scm.repo.git.add(A=True)
        scm.commit(f"dvc: commit experiment {exp_hash}")
        new_rev = scm.get_rev()
        scm.set_ref(branch, new_rev, old_ref=old_ref)
        scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
        return new_rev


class LocalExecutor(BaseExecutor):
    """Local machine experiment executor."""

    def __init__(
        self,
        *args,
        tmp_dir: Optional[str] = None,
        cache_dir: Optional[str] = None,
        **kwargs,
    ):
        self._tmp_dir = TemporaryDirectory(dir=tmp_dir)
        super().__init__(*args, root_dir=self._tmp_dir.name, **kwargs)
        if cache_dir:
            self._config(cache_dir)
        logger.debug(
            "Init local executor in dir '%s'", self._tmp_dir,
        )

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write("[core]\n    no_scm = true\n")
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    @property
    def git_url(self) -> str:
        root_dir = os.path.abspath(self.root_dir)
        if os.name == "nt":
            root_dir = root_dir.replace(os.sep, "/")
        return f"file://{root_dir}"

    def cleanup(self):
        super().cleanup()
        logger.debug("Removing tmpdir '%s'", self._tmp_dir)
        self._tmp_dir.cleanup()
