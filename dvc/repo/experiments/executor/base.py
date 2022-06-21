import logging
import os
import pickle
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from dvc.env import DVC_EXP_AUTO_PUSH, DVC_EXP_GIT_REMOTE
from dvc.exceptions import DvcException
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256, env2bool, relpath
from dvc.utils.fs import remove

from ..base import (
    EXEC_BASELINE,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    CheckpointExistsError,
    ExperimentExistsError,
    ExpRefInfo,
    UnchangedExperimentError,
)

if TYPE_CHECKING:
    from queue import Queue

    from scmrepo.git import Git

    from dvc.repo import Repo
    from dvc.stage import PipelineStage

    from ..base import ExpStashEntry

logger = logging.getLogger(__name__)


EXEC_TMP_DIR = "exps"
EXEC_PID_DIR = "run"


class ExecutorResult(NamedTuple):
    exp_hash: Optional[str]
    ref_info: Optional["ExpRefInfo"]
    force: bool


@dataclass
class ExecutorInfo:
    git_url: str
    baseline_rev: str
    location: str
    root_dir: str
    dvc_dir: str
    name: Optional[str] = None
    wdir: Optional[str] = None
    result_hash: Optional[str] = None
    result_ref: Optional[str] = None
    result_force: bool = False

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def asdict(self):
        return asdict(self)

    @property
    def result(self) -> Optional["ExecutorResult"]:
        if self.result_hash is None:
            return None
        return ExecutorResult(
            self.result_hash,
            ExpRefInfo.from_ref(self.result_ref) if self.result_ref else None,
            self.result_force,
        )

    def dump_json(self, filename: str):
        from dvc.utils.fs import makedirs
        from dvc.utils.serialize import modify_json

        makedirs(os.path.dirname(filename), exist_ok=True)
        with modify_json(filename) as d:
            d.update(self.asdict())


_T = TypeVar("_T", bound="BaseExecutor")


class BaseExecutor(ABC):
    """Base class for executing experiments in parallel.

    Parameters:
        root_dir: Path to SCM root.
        dvc_dir: Path to .dvc dir relative to SCM root.
        baseline_rev: Experiment baseline revision.
        wdir: Path to exec working directory relative to SCM root.
        name: Executor (experiment) name.
        result: Completed executor result.
    """

    PACKED_ARGS_FILE = "repro.dat"
    WARN_UNTRACKED = False
    QUIET = False
    INFOFILE_EXT = ".run"
    DEFAULT_LOCATION: str = "workspace"

    def __init__(
        self,
        root_dir: str,
        dvc_dir: str,
        baseline_rev: str,
        wdir: Optional[str] = None,
        name: Optional[str] = None,
        location: Optional[str] = None,
        result: Optional["ExecutorResult"] = None,
        **kwargs,
    ):
        self.dvc_dir = dvc_dir
        self.root_dir = root_dir
        self.wdir = wdir
        self.name = name
        self.baseline_rev = baseline_rev
        self.location: str = location or self.DEFAULT_LOCATION
        self.result = result

    @abstractmethod
    def init_git(self, scm: "Git", branch: Optional[str] = None):
        """Init git repo and populate it using exp refs from the specified
        SCM instance.
        """

    @property
    @abstractmethod
    def git_url(self) -> str:
        pass

    @abstractmethod
    def init_cache(self, repo: "Repo", rev: str, run_cache: bool = True):
        """Initialize DVC cache."""

    @abstractmethod
    def collect_cache(
        self, repo: "Repo", exp_ref: "ExpRefInfo", run_cache: bool = True
    ):
        """Collect DVC cache."""

    @property
    def info(self) -> "ExecutorInfo":
        if self.result is not None:
            result_dict: Dict[str, Any] = {
                "result_hash": self.result.exp_hash,
                "result_ref": (
                    str(self.result.ref_info) if self.result.ref_info else None
                ),
                "result_force": self.result.force,
            }
        else:
            result_dict = {}
        return ExecutorInfo(
            git_url=self.git_url,
            baseline_rev=self.baseline_rev,
            location=self.location,
            root_dir=self.root_dir,
            dvc_dir=self.dvc_dir,
            name=self.name,
            wdir=self.wdir,
            **result_dict,
        )

    @classmethod
    def from_info(cls: Type[_T], info: "ExecutorInfo") -> _T:
        if info.result_hash:
            result: Optional["ExecutorResult"] = ExecutorResult(
                info.result_hash,
                (
                    ExpRefInfo.from_ref(info.result_ref)
                    if info.result_ref
                    else None
                ),
                info.result_force,
            )
        else:
            result = None
        return cls(
            root_dir=info.root_dir,
            dvc_dir=info.dvc_dir,
            baseline_rev=info.baseline_rev,
            name=info.name,
            wdir=info.wdir,
            result=result,
        )

    @classmethod
    @abstractmethod
    def from_stash_entry(
        cls: Type[_T],
        repo: "Repo",
        stash_rev: str,
        entry: "ExpStashEntry",
        **kwargs,
    ) -> _T:
        pass

    @classmethod
    def _from_stash_entry(
        cls: Type[_T],
        repo: "Repo",
        stash_rev: str,
        entry: "ExpStashEntry",
        root_dir: str,
        **kwargs,
    ) -> _T:
        executor = cls(
            root_dir=root_dir,
            dvc_dir=relpath(repo.dvc_dir, repo.scm.root_dir),
            baseline_rev=entry.baseline_rev,
            name=entry.name,
            wdir=relpath(os.getcwd(), repo.scm.root_dir),
            **kwargs,
        )
        executor.init_git(repo.scm, branch=entry.branch)
        executor.init_cache(repo, stash_rev)
        return executor

    @staticmethod
    def hash_exp(stages: Iterable["PipelineStage"]) -> str:
        from dvc.stage import PipelineStage

        exp_data = {}
        for stage in stages:
            if isinstance(stage, PipelineStage):
                exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    def cleanup(self):
        pass

    # TODO: come up with better way to stash repro arguments
    @staticmethod
    def pack_repro_args(path, *args, fs=None, extra=None, **kwargs):
        dpath = os.path.dirname(path)
        if fs:
            open_func = fs.open
            fs.makedirs(dpath)
        else:
            from dvc.utils.fs import makedirs

            open_func = open
            makedirs(dpath, exist_ok=True)

        data = {"args": args, "kwargs": kwargs}
        if extra is not None:
            data["extra"] = extra
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
        on_diverged: Callable[[str, bool], None] = None,
        **kwargs,
    ) -> Iterable[str]:
        """Fetch reproduced experiment refs into the specified SCM.

        Args:
            dest_scm: Destination Git instance.
            force: If True, diverged refs will be overwritten
            on_diverged: Callback in the form on_diverged(ref, is_checkpoint)
                to be called when an experiment ref has diverged.

        Extra kwargs will be passed into the remote git client.
        """
        from ..utils import iter_remote_refs

        refs = []
        has_checkpoint = False
        for ref in iter_remote_refs(
            dest_scm,
            self.git_url,
            base=EXPS_NAMESPACE,
            **kwargs,
        ):
            if ref == EXEC_CHECKPOINT:
                has_checkpoint = True
            elif not ref.startswith(EXEC_NAMESPACE) and ref != EXPS_STASH:
                refs.append(ref)

        def on_diverged_ref(orig_ref: str, new_rev: str):
            if force:
                logger.debug("Replacing existing experiment '%s'", orig_ref)
                return True

            self._raise_ref_conflict(
                dest_scm, orig_ref, new_rev, has_checkpoint
            )
            if on_diverged:
                on_diverged(orig_ref, has_checkpoint)
            logger.debug("Reproduced existing experiment '%s'", orig_ref)
            return False

        # fetch experiments
        dest_scm.fetch_refspecs(
            self.git_url,
            [f"{ref}:{ref}" for ref in refs],
            on_diverged=on_diverged_ref,
            force=force,
            **kwargs,
        )
        # update last run checkpoint (if it exists)
        if has_checkpoint:
            dest_scm.fetch_refspecs(
                self.git_url,
                [f"{EXEC_CHECKPOINT}:{EXEC_CHECKPOINT}"],
                force=True,
                **kwargs,
            )
        return refs

    @classmethod
    def _validate_remotes(cls, dvc: "Repo", git_remote: Optional[str]):
        from scmrepo.exceptions import InvalidRemote

        from dvc.scm import InvalidRemoteSCMRepo

        if git_remote == dvc.root_dir:
            logger.warning(
                f"'{git_remote}' points to the current Git repo, experiment "
                "Git refs will not be pushed. But DVC cache and run cache "
                "will automatically be pushed to the default DVC remote "
                "(if any) on each experiment commit."
            )
        try:
            dvc.scm.validate_git_remote(git_remote)
        except InvalidRemote as exc:
            raise InvalidRemoteSCMRepo(str(exc))
        dvc.cloud.get_remote_odb()

    @classmethod
    def reproduce(
        cls,
        info: "ExecutorInfo",
        rev: str,
        queue: Optional["Queue"] = None,
        infofile: Optional[str] = None,
        log_errors: bool = True,
        log_level: Optional[int] = None,
        **kwargs,
    ) -> "ExecutorResult":
        """Run dvc repro and return the result.

        Returns tuple of (exp_hash, exp_ref, force) where exp_hash is the
            experiment hash (or None on error), exp_ref is the experiment ref,
            and force is a bool specifying whether or not this experiment
            should force overwrite any existing duplicates.
        """
        from dvc.repo.checkout import checkout as dvc_checkout
        from dvc.repo.reproduce import reproduce as dvc_reproduce
        from dvc.stage import PipelineStage

        auto_push = env2bool(DVC_EXP_AUTO_PUSH)
        git_remote = os.getenv(DVC_EXP_GIT_REMOTE, None)

        unchanged = []

        if queue is not None:
            queue.put((rev, os.getpid()))
        if log_errors and log_level is not None:
            cls._set_log_level(log_level)

        def filter_pipeline(stages):
            unchanged.extend(
                [stage for stage in stages if isinstance(stage, PipelineStage)]
            )

        exp_hash: Optional[str] = None
        exp_ref: Optional["ExpRefInfo"] = None
        repro_force: bool = False

        if infofile is not None:
            info.dump_json(infofile)

        with cls._repro_dvc(
            info,
            log_errors=log_errors,
            **kwargs,
        ) as dvc:
            if auto_push:
                cls._validate_remotes(dvc, git_remote)

            args, kwargs = cls._repro_args(dvc)
            if args:
                targets: Optional[Union[list, str]] = args[0]
            else:
                targets = kwargs.get("targets")

            repro_force = kwargs.get("force", False)
            logger.trace(  # type: ignore[attr-defined]
                "Executor repro with force = '%s'", str(repro_force)
            )

            repro_dry = kwargs.get("dry")

            # NOTE: checkpoint outs are handled as a special type of persist
            # out:
            #
            # - checkpoint out may not yet exist if this is the first time this
            #   experiment has been run, this is not an error condition for
            #   experiments
            # - if experiment was run with --reset, the checkpoint out will be
            #   removed at the start of the experiment (regardless of any
            #   dvc.lock entry for the checkpoint out)
            # - if run without --reset, the checkpoint out will be checked out
            #   using any hash present in dvc.lock (or removed if no entry
            #   exists in dvc.lock)
            checkpoint_reset: bool = kwargs.pop("reset", False)
            if not repro_dry:
                dvc_checkout(
                    dvc,
                    targets=targets,
                    with_deps=targets is not None,
                    force=True,
                    quiet=True,
                    allow_missing=True,
                    checkpoint_reset=checkpoint_reset,
                    recursive=kwargs.get("recursive", False),
                )

            checkpoint_func = partial(
                cls.checkpoint_callback,
                dvc,
                dvc.scm,
                info.name,
                repro_force or checkpoint_reset,
            )
            stages = dvc_reproduce(
                dvc,
                *args,
                on_unchanged=filter_pipeline,
                checkpoint_func=checkpoint_func,
                **kwargs,
            )

            exp_hash = cls.hash_exp(stages)
            if not repro_dry:
                ref, exp_ref, repro_force = cls._repro_commit(
                    dvc,
                    info,
                    stages,
                    exp_hash,
                    checkpoint_reset,
                    auto_push,
                    git_remote,
                    repro_force,
                )
            info.result_hash = exp_hash
            info.result_ref = ref
            info.result_force = repro_force

        if infofile is not None:
            info.dump_json(infofile)

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return ExecutorResult(exp_hash, exp_ref, repro_force)

    @classmethod
    def _repro_commit(
        cls,
        dvc,
        info,
        stages,
        exp_hash,
        checkpoint_reset,
        auto_push,
        git_remote,
        repro_force,
    ) -> Tuple[Optional[str], Optional["ExpRefInfo"], bool]:
        try:
            is_checkpoint = any(stage.is_checkpoint for stage in stages)
            if is_checkpoint and checkpoint_reset:
                # For reset checkpoint stages, we need to force
                # overwriting existing checkpoint refs even though
                # repro may not have actually been run with --force
                repro_force = True
            cls.commit(
                dvc.scm,
                exp_hash,
                exp_name=info.name,
                force=repro_force,
                checkpoint=is_checkpoint,
            )
            if auto_push:
                cls._auto_push(dvc, dvc.scm, git_remote)
        except UnchangedExperimentError:
            pass
        ref: Optional[str] = dvc.scm.get_ref(EXEC_BRANCH, follow=False)
        exp_ref: Optional["ExpRefInfo"] = (
            ExpRefInfo.from_ref(ref) if ref else None
        )
        if cls.WARN_UNTRACKED:
            untracked = dvc.scm.untracked_files()
            if untracked:
                logger.warning(
                    "The following untracked files were present in "
                    "the experiment directory after reproduction but "
                    "will not be included in experiment commits:\n"
                    "\t%s",
                    ", ".join(untracked),
                )
        return ref, exp_ref, repro_force

    @classmethod
    @contextmanager
    def _repro_dvc(
        cls,
        info: "ExecutorInfo",
        log_errors: bool = True,
        **kwargs,
    ):
        from dvc.repo import Repo
        from dvc.stage.monitor import CheckpointKilledError

        dvc = Repo(os.path.join(info.root_dir, info.dvc_dir))
        if cls.QUIET:
            dvc.scm_context.quiet = cls.QUIET
        old_cwd = os.getcwd()
        if info.wdir:
            os.chdir(os.path.join(dvc.scm.root_dir, info.wdir))
        else:
            os.chdir(dvc.root_dir)

        try:
            logger.debug("Running repro in '%s'", os.getcwd())
            yield dvc
        except CheckpointKilledError:
            raise
        except DvcException:
            if log_errors:
                logger.exception("")
            raise
        except Exception:
            if log_errors:
                logger.exception("unexpected error")
            raise
        finally:
            dvc.close()
            os.chdir(old_cwd)

    @classmethod
    def _repro_args(cls, dvc):
        args_path = os.path.join(dvc.tmp_dir, cls.PACKED_ARGS_FILE)
        if os.path.exists(args_path):
            args, kwargs = cls.unpack_repro_args(args_path)
            remove(args_path)
            # explicitly git rm/unstage the args file
            dvc.scm.add([args_path])
        else:
            args = []
            kwargs = {}
        return args, kwargs

    @staticmethod
    def _auto_push(
        dvc: "Repo",
        scm: "Git",
        git_remote: Optional[str],
        push_cache=True,
        run_cache=True,
    ):
        branch = scm.get_ref(EXEC_BRANCH, follow=False)
        try:
            dvc.experiments.push(
                git_remote,
                branch,
                push_cache=push_cache,
                run_cache=run_cache,
            )
        except BaseException as exc:  # pylint: disable=broad-except
            logger.warning(
                "Something went wrong while auto pushing experiment "
                f"to the remote '{git_remote}': {exc}"
            )

    @classmethod
    def checkpoint_callback(
        cls,
        dvc: "Repo",
        scm: "Git",
        name: Optional[str],
        force: bool,
        unchanged: Iterable["PipelineStage"],
        stages: Iterable["PipelineStage"],
    ):
        try:
            exp_hash = cls.hash_exp(list(stages) + list(unchanged))
            exp_rev = cls.commit(
                scm, exp_hash, exp_name=name, force=force, checkpoint=True
            )

            if env2bool(DVC_EXP_AUTO_PUSH):
                git_remote = os.getenv(DVC_EXP_GIT_REMOTE)
                cls._auto_push(dvc, scm, git_remote)
            logger.info("Checkpoint experiment iteration '%s'.", exp_rev[:7])
        except UnchangedExperimentError:
            pass

    @classmethod
    def commit(
        cls,
        scm: "Git",
        exp_hash: str,
        exp_name: Optional[str] = None,
        force: bool = False,
        checkpoint: bool = False,
    ):
        """Commit stages as an experiment and return the commit SHA."""
        rev = scm.get_rev()
        if not scm.is_dirty(untracked_files=False):
            logger.debug("No changes to commit")
            raise UnchangedExperimentError(rev)

        check_conflict = False
        branch = scm.get_ref(EXEC_BRANCH, follow=False)
        if branch:
            old_ref = rev
            logger.debug("Commit to current experiment branch '%s'", branch)
        else:
            baseline_rev = scm.get_ref(EXEC_BASELINE)
            name = exp_name if exp_name else f"exp-{exp_hash[:5]}"
            ref_info = ExpRefInfo(baseline_rev, name)
            branch = str(ref_info)
            old_ref = None
            if scm.get_ref(branch):
                if not force:
                    check_conflict = True
                logger.debug(
                    "%s existing experiment branch '%s'",
                    "Replace" if force else "Reuse",
                    branch,
                )
            else:
                logger.debug("Commit to new experiment branch '%s'", branch)

        scm.add([], update=True)
        scm.commit(f"dvc: commit experiment {exp_hash}", no_verify=True)
        new_rev = scm.get_rev()
        if check_conflict:
            new_rev = cls._raise_ref_conflict(scm, branch, new_rev, checkpoint)
        else:
            scm.set_ref(branch, new_rev, old_ref=old_ref)
        scm.set_ref(EXEC_BRANCH, branch, symbolic=True)
        if checkpoint:
            scm.set_ref(EXEC_CHECKPOINT, new_rev)
        return new_rev

    @staticmethod
    def _raise_ref_conflict(scm, ref, new_rev, checkpoint):
        # If this commit is a duplicate of the existing commit at 'ref', return
        # the existing commit. Otherwise, error out and require user to re-run
        # with --force as needed
        orig_rev = scm.get_ref(ref)
        if scm.diff(orig_rev, new_rev):
            if checkpoint:
                raise CheckpointExistsError(ref)
            raise ExperimentExistsError(ref)
        return orig_rev

    @staticmethod
    def _set_log_level(level):
        from dvc.logger import disable_other_loggers

        # When executor.reproduce is run in a multiprocessing child process,
        # dvc.cli.main will not be called for that child process so we need to
        # setup logging ourselves
        dvc_logger = logging.getLogger("dvc")
        disable_other_loggers()
        if level is not None:
            dvc_logger.setLevel(level)
