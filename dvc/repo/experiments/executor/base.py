import logging
import os
import pickle  # nosec B403
import shutil
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from enum import IntEnum
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from scmrepo.exceptions import SCMError

from dvc.env import DVC_EXP_AUTO_PUSH, DVC_EXP_GIT_REMOTE
from dvc.exceptions import DvcException
from dvc.repo.experiments.exceptions import ExperimentExistsError
from dvc.repo.experiments.refs import EXEC_BASELINE, EXEC_BRANCH, ExpRefInfo
from dvc.repo.experiments.utils import to_studio_params
from dvc.repo.metrics.show import _collect_top_level_metrics
from dvc.repo.params.show import _collect_top_level_params
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256, env2bool, relpath
from dvc.utils.fs import remove
from dvc.utils.studio import env_to_config

if TYPE_CHECKING:
    from queue import Queue

    from dvc.repo import Repo
    from dvc.repo.experiments.stash import ExpStashEntry
    from dvc.scm import Git
    from dvc.stage import PipelineStage, Stage

logger = logging.getLogger(__name__)


class ExecutorResult(NamedTuple):
    exp_hash: Optional[str]
    ref_info: Optional["ExpRefInfo"]
    force: bool


class TaskStatus(IntEnum):
    PENDING = 0
    PREPARING = 1
    RUNNING = 2
    SUCCESS = 3
    FAILED = 4
    CANCELED = 5
    FINISHED = 6


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
    status: TaskStatus = TaskStatus.PENDING

    @classmethod
    def from_dict(cls, d):
        if d.pop("collected", None):
            d["status"] = TaskStatus.FINISHED
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
        from dvc.utils.serialize import modify_json

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with modify_json(filename) as d:
            d.update(self.asdict())

    @classmethod
    def load_json(cls, filename: str) -> "ExecutorInfo":
        from dvc.utils.serialize import load_json

        return cls.from_dict(load_json(filename))


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
    INFOFILE_EXT = ".run"
    DEFAULT_LOCATION: str = "workspace"

    def __init__(
        self,
        root_dir: str,
        dvc_dir: str,
        baseline_rev: str,
        status: TaskStatus,
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
        self.status = status

    @abstractmethod
    def init_git(
        self,
        repo: "Repo",
        scm: "Git",
        stash_rev: str,
        entry: "ExpStashEntry",
        infofile: Optional[str],
        branch: Optional[str] = None,
    ):
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
            status=self.status,
            **result_dict,
        )

    @classmethod
    def from_info(cls: Type[_T], info: "ExecutorInfo") -> _T:
        if info.result_hash:
            result: Optional["ExecutorResult"] = ExecutorResult(
                info.result_hash,
                (ExpRefInfo.from_ref(info.result_ref) if info.result_ref else None),
                info.result_force,
            )
        else:
            result = None
        return cls(
            root_dir=info.root_dir,
            dvc_dir=info.dvc_dir,
            baseline_rev=info.baseline_rev,
            status=info.status,
            name=info.name,
            wdir=info.wdir,
            result=result,
        )

    @classmethod
    @abstractmethod
    def from_stash_entry(
        cls: Type[_T],
        repo: "Repo",
        entry: "ExpStashEntry",
        **kwargs,
    ) -> _T:
        pass

    @classmethod
    def _from_stash_entry(
        cls: Type[_T],
        repo: "Repo",
        entry: "ExpStashEntry",
        root_dir: str,
        **kwargs,
    ) -> _T:
        return cls(
            root_dir=root_dir,
            dvc_dir=relpath(repo.dvc_dir, repo.scm.root_dir),
            baseline_rev=entry.baseline_rev,
            status=TaskStatus.PREPARING,
            name=entry.name,
            wdir=relpath(os.getcwd(), repo.scm.root_dir),
            **kwargs,
        )

    @classmethod
    def _get_top_level_paths(cls, repo: "Repo") -> List["str"]:
        return list(
            chain(
                _collect_top_level_metrics(repo),
                _collect_top_level_params(repo),
                repo.index._plot_sources,  # pylint: disable=protected-access
            )
        )

    @classmethod
    def save(
        cls,
        info: "ExecutorInfo",
        force: bool = False,
        include_untracked: Optional[List[str]] = None,
        message: Optional[str] = None,
    ) -> ExecutorResult:
        from dvc.dvcfile import LOCK_FILE
        from dvc.repo import Repo

        exp_hash: Optional[str] = None
        exp_ref: Optional[ExpRefInfo] = None

        dvc = Repo(os.path.join(info.root_dir, info.dvc_dir))
        old_cwd = os.getcwd()
        if info.wdir:
            os.chdir(os.path.join(dvc.scm.root_dir, info.wdir))
        else:
            os.chdir(dvc.root_dir)

        include_untracked = include_untracked or []
        include_untracked.extend(cls._get_top_level_paths(dvc))
        # dvc repro automatically stages dvc.lock. Running redundant `git add`
        # on it causes an error when exiting the detached head context.
        if LOCK_FILE in dvc.scm.untracked_files():
            include_untracked.append(LOCK_FILE)

        try:
            stages = dvc.commit([], force=True, relink=False)
            exp_hash = cls.hash_exp(stages)
            if include_untracked:
                dvc.scm.add(include_untracked, force=True)  # type: ignore[call-arg]
            cls.commit(
                dvc.scm,  # type: ignore[arg-type]
                exp_hash,
                exp_name=info.name,
                force=force,
                message=message,
            )
            ref: Optional[str] = dvc.scm.get_ref(EXEC_BRANCH, follow=False)
            exp_ref = ExpRefInfo.from_ref(ref) if ref else None
            untracked = dvc.scm.untracked_files()
            if untracked:
                logger.warning(
                    "The following untracked files were present in "
                    "the workspace before saving but "
                    "will not be included in the experiment commit:\n"
                    "\t%s",
                    ", ".join(untracked),
                )
            info.result_hash = exp_hash
            info.result_ref = ref
            info.result_force = False
            info.status = TaskStatus.SUCCESS
        except DvcException:
            info.status = TaskStatus.FAILED
            raise
        finally:
            dvc.close()
            os.chdir(old_cwd)

        return ExecutorResult(ref, exp_ref, info.result_force)

    @staticmethod
    def hash_exp(stages: Iterable["PipelineStage"]) -> str:
        from dvc.stage import PipelineStage

        exp_data = {}
        for stage in stages:
            if isinstance(stage, PipelineStage):
                exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    def cleanup(self, infofile: Optional[str] = None):
        if infofile is not None:
            info = ExecutorInfo.load_json(infofile)
            if info.status < TaskStatus.FAILED:
                info.status = TaskStatus.FINISHED
            info.dump_json(infofile)

    # TODO: come up with better way to stash repro arguments
    @staticmethod
    def pack_repro_args(path, *args, fs=None, extra=None, **kwargs):
        dpath = os.path.dirname(path)
        if fs:
            open_func = fs.open
            fs.makedirs(dpath)
        else:
            open_func = open
            os.makedirs(dpath, exist_ok=True)

        data = {"args": args, "kwargs": kwargs}
        if extra is not None:
            data["extra"] = extra
        with open_func(path, "wb") as fobj:
            pickle.dump(data, fobj)

    @staticmethod
    def unpack_repro_args(path):
        with open(path, "rb") as fobj:
            data = pickle.load(fobj)  # noqa: S301 # nosec B301
        return data["args"], data["kwargs"]

    def fetch_exps(
        self,
        dest_scm: "Git",
        refs: List[str],
        force: bool = False,
        on_diverged: Optional[Callable[[str], None]] = None,
        **kwargs,
    ) -> Iterable[str]:
        """Fetch reproduced experiment refs into the specified SCM.

        Args:
            dest_scm: Destination Git instance.
            refs: reference names to be fetched from the remotes.
            force: If True, diverged refs will be overwritten
            on_diverged: Callback in the form on_diverged(ref)
                to be called when an experiment ref has diverged.

        Extra kwargs will be passed into the remote git client.
        """

        def on_diverged_ref(orig_ref: str, new_rev: str):
            if force:
                logger.debug("Replacing existing experiment '%s'", orig_ref)
                return True

            if on_diverged:
                return on_diverged(orig_ref)

            self._raise_ref_conflict(dest_scm, orig_ref, new_rev)
            logger.debug("Reproduced existing experiment '%s'", orig_ref)
            return False

        # fetch experiments
        try:
            refspecs = [f"{ref}:{ref}" for ref in refs]
            dest_scm.fetch_refspecs(
                self.git_url,
                refspecs,
                on_diverged=on_diverged_ref,
                force=force,
                **kwargs,
            )
        except SCMError:
            pass

        return refs

    @classmethod
    def _validate_remotes(cls, dvc: "Repo", git_remote: Optional[str]):
        from scmrepo.exceptions import InvalidRemote

        from dvc.scm import InvalidRemoteSCMRepo

        if git_remote == dvc.root_dir:
            logger.warning(
                (
                    "'%s' points to the current Git repo, experiment "
                    "Git refs will not be pushed. But DVC cache and run cache "
                    "will automatically be pushed to the default DVC remote "
                    "(if any) on each experiment commit."
                ),
                git_remote,
            )
        try:
            dvc.scm.validate_git_remote(git_remote)
        except InvalidRemote as exc:
            raise InvalidRemoteSCMRepo(str(exc))  # noqa: B904
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
        copy_paths: Optional[List[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ) -> "ExecutorResult":
        """Run dvc repro and return the result.

        Returns tuple of (exp_hash, exp_ref, force) where exp_hash is the
            experiment hash (or None on error), exp_ref is the experiment ref,
            and force is a bool specifying whether or not this experiment
            should force overwrite any existing duplicates.
        """
        from dvc.repo.checkout import checkout as dvc_checkout
        from dvc.ui import ui

        auto_push = env2bool(DVC_EXP_AUTO_PUSH)
        git_remote = os.getenv(DVC_EXP_GIT_REMOTE, None)

        if queue is not None:
            queue.put((rev, os.getpid()))
        if log_errors and log_level is not None:
            cls._set_log_level(log_level)

        exp_hash: Optional[str] = None
        exp_ref: Optional["ExpRefInfo"] = None
        repro_force: bool = False

        if info.name:
            ui.write(f"Reproducing experiment '{info.name}'")

        with cls._repro_dvc(
            info,
            infofile,
            log_errors=log_errors,
            copy_paths=copy_paths,
            message=message,
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

            if not repro_dry:
                dvc_checkout(
                    dvc,
                    targets=targets,
                    with_deps=targets is not None,
                    force=True,
                    allow_missing=True,
                    recursive=kwargs.get("recursive", False),
                )

            kwargs["repro_fn"] = cls._repro_and_track
            stages = dvc.reproduce(*args, **kwargs)
            if paths := cls._get_top_level_paths(dvc):
                logger.debug("Staging top-level files: %s", paths)
                dvc.scm_context.add(paths)

            exp_hash = cls.hash_exp(stages)
            if not repro_dry:
                ref, exp_ref, repro_force = cls._repro_commit(
                    dvc,
                    info,
                    exp_hash,
                    auto_push,
                    git_remote,
                    repro_force,
                    message=message,
                )
                info.result_hash = exp_hash
                info.result_ref = ref
                info.result_force = repro_force

        # ideally we would return stages here like a normal repro() call, but
        # stages is not currently picklable and cannot be returned across
        # multiprocessing calls
        return ExecutorResult(exp_hash, exp_ref, repro_force)

    @staticmethod
    def _repro_and_track(stage: "Stage", **kwargs) -> Optional["Stage"]:
        from dvc.repo.reproduce import _reproduce_stage
        from dvc.stage.utils import _get_stage_files

        ret = _reproduce_stage(stage, **kwargs)
        if not kwargs.get("dry") and (paths := _get_stage_files(stage)):
            logger.debug("Staging stage-related files: %s", paths)
            stage.repo.scm_context.add(paths)
        return ret

    @classmethod
    def _repro_commit(
        cls,
        dvc,
        info,
        exp_hash,
        auto_push,
        git_remote,
        repro_force,
        message: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional["ExpRefInfo"], bool]:
        cls.commit(
            dvc.scm,
            exp_hash,
            exp_name=info.name,
            force=repro_force,
            message=message,
        )
        if auto_push:
            cls._auto_push(dvc, dvc.scm, git_remote)
        ref: Optional[str] = dvc.scm.get_ref(EXEC_BRANCH, follow=False)
        exp_ref: Optional["ExpRefInfo"] = ExpRefInfo.from_ref(ref) if ref else None
        if cls.WARN_UNTRACKED:
            untracked = dvc.scm.untracked_files()
            if untracked:
                logger.warning(
                    (
                        "The following untracked files were present in "
                        "the experiment directory after reproduction but "
                        "will not be included in experiment commits:\n"
                        "\t%s"
                    ),
                    ", ".join(untracked),
                )
        return ref, exp_ref, repro_force

    @classmethod
    @contextmanager
    def _repro_dvc(  # noqa: C901
        cls,
        info: "ExecutorInfo",
        infofile: Optional[str] = None,
        log_errors: bool = True,
        copy_paths: Optional[List[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ) -> Iterator["Repo"]:
        from dvc_studio_client.post_live_metrics import post_live_metrics

        from dvc.repo import Repo

        with Repo(os.path.join(info.root_dir, info.dvc_dir)) as dvc:
            info.status = TaskStatus.RUNNING
            if infofile is not None:
                info.dump_json(infofile)
            dvc.scm_context.quiet = True
            old_cwd = os.getcwd()

            for path in copy_paths or []:
                cls._copy_path(os.path.abspath(path), os.path.join(dvc.root_dir, path))

            if info.wdir:
                os.chdir(os.path.join(dvc.scm.root_dir, info.wdir))
            else:
                os.chdir(dvc.root_dir)

            args_path = os.path.join(dvc.tmp_dir, cls.PACKED_ARGS_FILE)
            if os.path.exists(args_path):
                _, kwargs = cls.unpack_repro_args(args_path)
            dvc_studio_config = dvc.config.get("studio")
            # set missing config options using saved config
            # inferring repo url will fail if not set here
            run_env_config = env_to_config(kwargs.get("run_env", {}))
            dvc_studio_config = {**run_env_config, **dvc_studio_config}
            try:
                post_live_metrics(
                    "start",
                    info.baseline_rev,
                    info.name,  # type: ignore[arg-type]
                    "dvc",
                    params=to_studio_params(dvc.params.show()),
                    dvc_studio_config=dvc_studio_config,
                    message=message,
                )
                logger.debug("Running repro in '%s'", os.getcwd())
                yield dvc
                info.status = TaskStatus.SUCCESS
            except DvcException:
                if log_errors:
                    logger.exception("")
                info.status = TaskStatus.FAILED
                raise
            except Exception:
                if log_errors:
                    logger.exception("unexpected error")
                info.status = TaskStatus.FAILED
                raise
            finally:
                from dvc.repo.metrics.show import _gather_metrics

                post_live_metrics(
                    "done",
                    info.baseline_rev,
                    info.name,  # type: ignore[arg-type]
                    "dvc",
                    experiment_rev=dvc.experiments.scm.get_ref(EXEC_BRANCH),
                    metrics=_gather_metrics(dvc, on_error="return"),
                    dvc_studio_config=dvc_studio_config,
                )

                if infofile is not None:
                    info.dump_json(infofile)
                os.chdir(old_cwd)

    @classmethod
    def _repro_args(cls, dvc):
        args_path = os.path.join(dvc.tmp_dir, cls.PACKED_ARGS_FILE)
        if os.path.exists(args_path):
            args, kwargs = cls.unpack_repro_args(args_path)
            remove(args_path)
            # explicitly git rm/unstage the args file
            dvc.scm.add([args_path], force=True)
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
        except BaseException as exc:  # noqa: BLE001, pylint: disable=W0703
            logger.warning(
                (
                    "Something went wrong while auto pushing experiment "
                    "to the remote '%s': %s"
                ),
                git_remote,
                exc,
            )

    @classmethod
    def commit(
        cls,
        scm: "Git",
        exp_hash: str,
        exp_name: Optional[str] = None,
        force: bool = False,
        message: Optional[str] = None,
    ):
        """Commit stages as an experiment and return the commit SHA."""
        rev = scm.get_rev()
        if not scm.is_dirty(untracked_files=False):
            logger.debug("No changes to commit")

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
        message = message or f"dvc: commit experiment {exp_hash}"
        scm.commit(message, no_verify=True)
        new_rev = scm.get_rev()
        if check_conflict:
            new_rev = cls._raise_ref_conflict(scm, branch, new_rev)
        else:
            scm.set_ref(branch, new_rev, old_ref=old_ref)
        scm.set_ref(EXEC_BRANCH, branch, symbolic=True)

        return new_rev

    @staticmethod
    def _raise_ref_conflict(scm, ref, new_rev):
        # If this commit is a duplicate of the existing commit at 'ref', return
        # the existing commit. Otherwise, error out and require user to re-run
        # with --force as needed
        orig_rev = scm.get_ref(ref)
        if scm.diff(orig_rev, new_rev):
            raise ExperimentExistsError(ref)
        return orig_rev

    @staticmethod
    def _set_log_level(level):
        # When executor.reproduce is run in a multiprocessing child process,
        # dvc.cli.main will not be called for that child process so we need to
        # setup logging ourselves
        dvc_logger = logging.getLogger("dvc")
        if level is not None:
            dvc_logger.setLevel(level)

    @staticmethod
    def _copy_path(src, dst):
        try:
            if os.path.isfile(src):
                shutil.copy(src, dst)
            elif os.path.isdir(src):
                shutil.copytree(src, dst)
            else:
                raise DvcException(
                    f"Unable to copy '{src}'. It is not a file or directory."
                )
        except OSError as exc:
            raise DvcException(f"Unable to copy '{src}' to '{dst}'.") from exc

    @contextmanager
    def set_temp_refs(self, scm: "Git", temp_dict: Dict[str, str]):
        try:
            for ref, rev in temp_dict.items():
                scm.set_ref(ref, rev)
            yield
        finally:
            for ref in temp_dict:
                if scm.get_ref(ref):
                    scm.remove_ref(ref)
