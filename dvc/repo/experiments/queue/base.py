import logging
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Type,
    Union,
)

from funcy import cached_property, retry

from dvc.dependency import ParamsDependency
from dvc.env import DVCLIVE_RESUME
from dvc.exceptions import DvcException
from dvc.lock import LockError
from dvc.ui import ui

from ..exceptions import CheckpointExistsError, ExperimentExistsError
from ..executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorResult,
)
from ..executor.local import WorkspaceExecutor
from ..refs import ExpRefInfo
from ..stash import ExpStash, ExpStashEntry
from ..utils import exp_refs_by_rev, scm_locked

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.repo import Repo
    from dvc.repo.experiments import Experiments

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueueEntry:
    dvc_root: str
    scm_root: str
    stash_ref: str
    stash_rev: str
    baseline_rev: str
    branch: Optional[str]
    name: Optional[str]
    head_rev: Optional[str] = None

    def __eq__(self, other: object):
        return (
            isinstance(other, QueueEntry)
            and self.dvc_root == other.dvc_root
            and self.scm_root == other.scm_root
            and self.stash_ref == other.stash_ref
            and self.stash_rev == other.stash_rev
        )

    def asdict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "QueueEntry":
        return cls(**d)


class QueueGetResult(NamedTuple):
    entry: QueueEntry
    executor: BaseExecutor


class QueueDoneResult(NamedTuple):
    entry: QueueEntry
    result: Optional[ExecutorResult]


class BaseStashQueue(ABC):
    """Naive Git-stash based experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    def __init__(
        self, repo: "Repo", ref: str, failed_ref: Optional[str] = None
    ):
        """Construct a queue.

        Arguments:
            scm: Git SCM instance for this queue.
            ref: Git stash ref for this queue.
            failed_ref: Failed run Git stash ref for this queue.
        """
        self.repo = repo
        self.ref = ref
        self.failed_ref = failed_ref

    @property
    def scm(self) -> "Git":
        return self.repo.scm

    @cached_property
    def stash(self) -> ExpStash:
        return ExpStash(self.scm, self.ref)

    @cached_property
    def failed_stash(self) -> Optional[ExpStash]:
        return ExpStash(self.scm, self.failed_ref) if self.failed_ref else None

    @cached_property
    def pid_dir(self) -> str:
        return os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR, EXEC_PID_DIR)

    @cached_property
    def args_file(self):
        return os.path.join(self.repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)

    @abstractmethod
    def put(self, *args, **kwargs) -> QueueEntry:
        """Stash an experiment and add it to the queue."""

    @abstractmethod
    def get(self) -> QueueGetResult:
        """Pop and return the first item in the queue."""

    def remove(
        self,
        revs: Collection[str],
        all_: bool = False,
        queued: bool = False,
        **kwargs,
    ) -> List[str]:
        """Remove the specified entries from the queue.

        Arguments:
            revs: Stash revisions or queued exp names to be removed.
            queued: Remove all queued tasks.
            all: Remove all tasks.

        Returns:
            Revisions (or names) which were removed.
        """

        if all_ or queued:
            return self.clear()

        name_to_remove: List[str] = []
        entry_to_remove: List[ExpStashEntry] = []
        queue_entries = self.match_queue_entry_by_name(
            revs, self.iter_queued()
        )
        for name, entry in queue_entries.items():
            if entry:
                entry_to_remove.append(self.stash.stash_revs[entry.stash_rev])
                name_to_remove.append(name)

        self.stash.remove_revs(entry_to_remove)
        return name_to_remove

    def clear(self, **kwargs) -> List[str]:
        """Remove all entries from the queue."""
        stash_revs = self.stash.stash_revs
        name_to_remove = list(stash_revs)
        self.stash.remove_revs(list(stash_revs.values()))

        return name_to_remove

    def status(self) -> List[Dict[str, Any]]:
        """Show the status of exp tasks in queue"""
        from datetime import datetime

        result: List[Dict[str, Optional[str]]] = []

        def _get_timestamp(rev: str) -> datetime:
            commit = self.scm.resolve_commit(rev)
            return datetime.fromtimestamp(commit.commit_time)

        def _format_entry(
            entry: QueueEntry,
            exp_result: Optional[ExecutorResult] = None,
            status: str = "Unknown",
        ) -> Dict[str, Any]:
            name = entry.name
            if not name and exp_result and exp_result.ref_info:
                name = exp_result.ref_info.name
            # NOTE: We fallback to Unknown status for experiments
            # generated in prior (incompatible) DVC versions
            return {
                "rev": entry.stash_rev,
                "name": name,
                "timestamp": _get_timestamp(entry.stash_rev),
                "status": status,
            }

        result.extend(
            _format_entry(queue_entry, status="Running")
            for queue_entry in self.iter_active()
        )
        result.extend(
            _format_entry(queue_entry, status="Queued")
            for queue_entry in self.iter_queued()
        )
        result.extend(
            _format_entry(queue_entry, status="Failed")
            for queue_entry, _ in self.iter_failed()
        )
        result.extend(
            _format_entry(queue_entry, exp_result=exp_result, status="Success")
            for queue_entry, exp_result in self.iter_success()
        )
        return result

    @abstractmethod
    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        """Iterate over items in the queue."""

    @abstractmethod
    def iter_active(self) -> Generator[QueueEntry, None, None]:
        """Iterate over items which are being actively processed."""

    @abstractmethod
    def iter_done(self) -> Generator[QueueDoneResult, None, None]:
        """Iterate over items which been processed."""

    @abstractmethod
    def iter_success(self) -> Generator[QueueDoneResult, None, None]:
        """Iterate over items which been success."""

    @abstractmethod
    def iter_failed(self) -> Generator[QueueDoneResult, None, None]:
        """Iterate over items which been failed."""

    @abstractmethod
    def reproduce(self) -> Mapping[str, Mapping[str, str]]:
        """Reproduce queued experiments sequentially."""

    @abstractmethod
    def get_result(self, entry: QueueEntry) -> Optional[ExecutorResult]:
        """Return result of the specified item.

        This method blocks until the specified item has been collected.
        """

    @abstractmethod
    def kill(self, revs: str) -> None:
        """Kill the specified running entries in the queue.

        Arguments:
            revs: Stash revs or running exp name to be killed.
        """

    @abstractmethod
    def shutdown(self, kill: bool = False):
        """Shutdown the queue worker.

        Arguments:
            kill: If True, the any active experiments will be killed and the
                worker will shutdown immediately. If False, the worker will
                finish any active experiments before shutting down.
        """

    @abstractmethod
    def logs(
        self,
        rev: str,
        encoding: Optional[str] = None,
        follow: bool = False,
    ):
        """Print redirected output logs for an exp process.

        Args:
            rev: Stash rev or exp name.
            encoding: Text encoding for redirected output. Defaults to
                `locale.getpreferredencoding()`.
            follow: Attach to running exp process and follow additional
                output.
        """

    def _stash_exp(
        self,
        *args,
        params: Optional[Dict[str, List[str]]] = None,
        resume_rev: Optional[str] = None,
        baseline_rev: Optional[str] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> QueueEntry:
        """Stash changes from the workspace as an experiment.

        Args:
            params: Dict mapping paths to `Hydra Override`_ patterns,
                provided via `exp run --set-param`.
            resume_rev: Optional checkpoint resume rev.
            baseline_rev: Optional baseline rev for this experiment, defaults
                to the current SCM rev.
            branch: Optional experiment branch name. If specified, the
                experiment will be added to `branch` instead of creating
                a new branch.
            name: Optional experiment name. If specified this will be used as
                the human-readable name in the experiment branch ref. Has no
                effect of branch is specified.

        .. _Hydra Override:
            https://hydra.cc/docs/next/advanced/override_grammar/basic/
        """
        with self.scm.detach_head(client="dvc") as orig_head:
            stash_head = orig_head
            if baseline_rev is None:
                baseline_rev = orig_head

            with self.scm.stash_workspace() as workspace:
                try:
                    if workspace:
                        self.stash.apply(workspace)

                    if resume_rev:
                        # move HEAD to the resume rev so that the stashed diff
                        # only contains changes relative to resume rev
                        stash_head = resume_rev
                        self.scm.set_ref(
                            "HEAD",
                            resume_rev,
                            message=f"dvc: resume from HEAD {resume_rev[:7]}",
                        )
                        self.scm.reset()

                    # update experiment params from command line
                    if params:
                        self._update_params(params)

                    # DVC commit data deps to preserve state across workspace
                    # & tempdir runs
                    self._stash_commit_deps(*args, **kwargs)

                    if resume_rev:
                        if branch:
                            branch_name = ExpRefInfo.from_ref(branch).name
                        else:
                            branch_name = f"{resume_rev[:7]}"
                        if self.scm.is_dirty(untracked_files=False):
                            ui.write(
                                "Modified checkpoint experiment based on "
                                f"'{branch_name}' will be created",
                            )
                            branch = None
                        elif (
                            not branch
                            or self.scm.get_ref(branch) != resume_rev
                        ):
                            err_msg = [
                                (
                                    "Nothing to do for unchanged checkpoint "
                                    f"'{resume_rev[:7]}'. "
                                )
                            ]
                            if branch:
                                err_msg.append(
                                    "To resume from the head of this "
                                    "experiment, use "
                                    f"'dvc exp apply {branch_name}'."
                                )
                            else:
                                names = [
                                    ref_info.name
                                    for ref_info in exp_refs_by_rev(
                                        self.scm, resume_rev
                                    )
                                ]
                                if len(names) > 3:
                                    names[3:] = [
                                        f"... ({len(names) - 3} more)"
                                    ]
                                err_msg.append(
                                    "To resume an experiment containing this "
                                    "checkpoint, apply one of these heads:\n"
                                    "\t{}".format(", ".join(names))
                                )
                            raise DvcException("".join(err_msg))
                        else:
                            ui.write(
                                "Existing checkpoint experiment "
                                f"'{branch_name}' will be resumed"
                            )
                        if name:
                            logger.warning(
                                "Ignoring option '--name %s' for resumed "
                                "experiment. Existing experiment name will"
                                "be preserved instead.",
                                name,
                            )

                    # save additional repro command line arguments
                    run_env = {DVCLIVE_RESUME: "1"} if resume_rev else {}
                    self._pack_args(*args, run_env=run_env, **kwargs)

                    # save experiment as a stash commit
                    msg = self._stash_msg(
                        stash_head,
                        baseline_rev=baseline_rev,
                        branch=branch,
                        name=name,
                    )
                    stash_rev = self.stash.push(message=msg)
                    logger.debug(
                        (
                            "Stashed experiment '%s' with baseline '%s' "
                            "for future execution."
                        ),
                        stash_rev[:7],
                        baseline_rev[:7],
                    )
                finally:
                    if resume_rev:
                        # NOTE: this set_ref + reset() is equivalent to
                        # `git reset orig_head` (our SCM reset() only operates
                        # on HEAD rather than any arbitrary commit)
                        self.scm.set_ref(
                            "HEAD", orig_head, message="dvc: restore HEAD"
                        )
                        self.scm.reset()
                    # Revert any of our changes before prior unstashing
                    self.scm.reset(hard=True)

        return QueueEntry(
            self.repo.root_dir,
            self.scm.root_dir,
            self.ref,
            stash_rev,
            baseline_rev,
            branch,
            name,
            stash_head,
        )

    def _stash_commit_deps(self, *args, **kwargs):
        if len(args):
            targets = args[0]
        else:
            targets = kwargs.get("targets")
        if isinstance(targets, str):
            targets = [targets]
        elif not targets:
            targets = [None]
        for target in targets:
            self.repo.commit(
                target,
                with_deps=True,
                recursive=kwargs.get("recursive", False),
                force=True,
                allow_missing=True,
                data_only=True,
            )

    @staticmethod
    def _stash_msg(
        rev: str,
        baseline_rev: str,
        branch: Optional[str] = None,
        name: Optional[str] = None,
    ) -> str:
        if not baseline_rev:
            baseline_rev = rev
        msg = ExpStash.format_message(rev, baseline_rev, name)
        if branch:
            return f"{msg}:{branch}"
        return msg

    def _pack_args(self, *args, **kwargs) -> None:
        import pickle

        if os.path.exists(self.args_file) and self.scm.is_tracked(
            self.args_file
        ):
            logger.warning(
                (
                    "Temporary DVC file '.dvc/tmp/%s' exists and was "
                    "likely committed to Git by mistake. It should be removed "
                    "with:\n"
                    "\tgit rm .dvc/tmp/%s"
                ),
                BaseExecutor.PACKED_ARGS_FILE,
                BaseExecutor.PACKED_ARGS_FILE,
            )
            with open(self.args_file, "rb") as fobj:
                try:
                    data = pickle.load(fobj)
                except Exception:  # pylint: disable=broad-except
                    data = {}
            extra = int(data.get("extra", 0)) + 1
        else:
            extra = None
        BaseExecutor.pack_repro_args(
            self.args_file, *args, extra=extra, **kwargs
        )
        self.scm.add(self.args_file)

    @staticmethod
    def _format_new_params_msg(new_params, config_path):
        """Format an error message for when new parameters are identified"""
        new_param_count = len(new_params)
        pluralise = "s are" if new_param_count > 1 else " is"
        param_list = ", ".join(new_params)
        return (
            f"{new_param_count} parameter{pluralise} missing "
            f"from '{config_path}': {param_list}"
        )

    def _update_params(self, params: Dict[str, List[str]]):
        """Update param files with the provided `Hydra Override`_ patterns.

        Args:
            params: Dict mapping paths to `Hydra Override`_ patterns,
                provided via `exp run --set-param`.

        .. _Hydra Override:
            https://hydra.cc/docs/advanced/override_grammar/basic/
        """
        from dvc.utils.hydra import apply_overrides, compose_and_dump

        logger.debug("Using experiment params '%s'", params)

        hydra_config = self.repo.config.get("hydra", {})
        hydra_enabled = hydra_config.get("enabled", False)
        hydra_output_file = ParamsDependency.DEFAULT_PARAMS_FILE
        for path, overrides in params.items():
            if hydra_enabled and path == hydra_output_file:
                config_dir = os.path.join(
                    self.repo.root_dir, hydra_config.get("config_dir", "conf")
                )
                config_name = hydra_config.get("config_name", "config")
                compose_and_dump(
                    path,
                    config_dir,
                    config_name,
                    overrides,
                )
            else:
                apply_overrides(path, overrides)

        # Force params file changes to be staged in git
        # Otherwise in certain situations the changes to params file may be
        # ignored when we `git stash` them since mtime is used to determine
        # whether the file is dirty
        self.scm.add(list(params.keys()))

    @staticmethod
    @retry(180, errors=LockError, timeout=1)
    @scm_locked
    def init_executor(
        exp: "Experiments",
        queue_entry: QueueEntry,
        executor_cls: Type[BaseExecutor] = WorkspaceExecutor,
        **kwargs,
    ) -> BaseExecutor:
        scm = exp.scm
        stash = ExpStash(scm, queue_entry.stash_ref)
        stash_rev = queue_entry.stash_rev
        stash_entry = stash.stash_revs.get(
            stash_rev,
            ExpStashEntry(None, stash_rev, stash_rev, None, None),
        )
        if stash_entry.stash_index is not None:
            stash.drop(stash_entry.stash_index)
        executor = executor_cls.from_stash_entry(
            exp.repo, stash_entry, **kwargs
        )

        infofile = exp.celery_queue.get_infofile_path(stash_rev)
        executor.init_git(
            exp.repo.scm,
            stash_rev,
            stash_entry,
            infofile,
            branch=stash_entry.branch,
        )

        executor.init_cache(exp.repo, stash_rev)

        return executor

    def get_infofile_path(self, name: str) -> str:
        return os.path.join(
            self.pid_dir,
            name,
            f"{name}{BaseExecutor.INFOFILE_EXT}",
        )

    @staticmethod
    @retry(180, errors=LockError, timeout=1)
    @scm_locked
    def collect_git(
        exp: "Experiments",
        executor: BaseExecutor,
        exec_result: ExecutorResult,
    ) -> Dict[str, str]:
        results = {}

        def on_diverged(ref: str, checkpoint: bool):
            ref_info = ExpRefInfo.from_ref(ref)
            if checkpoint:
                raise CheckpointExistsError(ref_info.name)
            raise ExperimentExistsError(ref_info.name)

        for ref in executor.fetch_exps(
            exp.scm,
            force=exec_result.force,
            on_diverged=on_diverged,
        ):
            exp_rev = exp.scm.get_ref(ref)
            if exp_rev:
                assert exec_result.exp_hash
                logger.debug("Collected experiment '%s'.", exp_rev[:7])
                results[exp_rev] = exec_result.exp_hash

        return results

    @classmethod
    def collect_executor(
        cls,
        exp: "Experiments",
        executor: BaseExecutor,
        exec_result: ExecutorResult,
    ) -> Dict[str, str]:
        results = cls.collect_git(exp, executor, exec_result)

        if exec_result.ref_info is not None:
            executor.collect_cache(exp.repo, exec_result.ref_info)

        return results

    def match_queue_entry_by_name(
        self,
        exp_names: Collection[str],
        *entries: Iterable[Union[QueueEntry, QueueDoneResult]],
    ) -> Dict[str, Optional[QueueEntry]]:
        from funcy import concat

        entry_name_dict: Dict[str, QueueEntry] = {}
        entry_rev_dict: Dict[str, QueueEntry] = {}
        for entry in concat(*entries):
            if isinstance(entry, QueueDoneResult):
                queue_entry: QueueEntry = entry.entry
                if (
                    entry.result is not None
                    and entry.result.ref_info is not None
                ):
                    name: Optional[str] = entry.result.ref_info.name
                else:
                    name = queue_entry.name
            else:
                queue_entry = entry
                name = queue_entry.name
            if name:
                entry_name_dict[name] = queue_entry
            entry_rev_dict[queue_entry.stash_rev] = queue_entry

        result: Dict[str, Optional[QueueEntry]] = {}
        for exp_name in exp_names:
            result[exp_name] = None
            if exp_name in entry_name_dict:
                result[exp_name] = entry_name_dict[exp_name]
                continue
            if self.scm.is_sha(exp_name):
                for rev in entry_rev_dict:
                    if rev.startswith(exp_name.lower()):
                        result[exp_name] = entry_rev_dict[rev]
                        break

        return result

    def stash_failed(self, entry: QueueEntry) -> None:
        """Add an entry to the failed exp stash.

        Arguments:
            entry: Failed queue entry to add. ``entry.stash_rev`` must be a
                valid Git stash commit.
        """
        if self.failed_stash is not None:
            assert entry.head_rev
            logger.debug("Stashing failed exp '%s'", entry.stash_rev[:7])
            msg = self.failed_stash.format_message(
                entry.head_rev,
                baseline_rev=entry.baseline_rev,
                name=entry.name,
                branch=entry.branch,
            )
            self.scm.set_ref(
                self.failed_stash.ref,
                entry.stash_rev,
                message=f"commit: {msg}",
            )

    @abstractmethod
    def get_running_exps(self, fetch_refs: bool = True) -> Dict[str, Dict]:
        """Get the execution info of the currently running experiments

        Args:
            fetch_ref (bool): fetch completed checkpoints or not.
        """
