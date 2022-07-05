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
    List,
    Mapping,
    NamedTuple,
    Optional,
    Type,
)

from funcy import cached_property

from dvc.dependency.param import MissingParamsError
from dvc.env import DVCLIVE_RESUME
from dvc.exceptions import DvcException
from dvc.ui import ui

from ..exceptions import CheckpointExistsError, ExperimentExistsError
from ..executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorResult,
)
from ..executor.local import WorkspaceExecutor
from ..refs import EXEC_BASELINE, EXEC_HEAD, EXEC_MERGE, ExpRefInfo
from ..stash import ExpStash, ExpStashEntry
from ..utils import exp_refs_by_rev, scm_locked

if TYPE_CHECKING:
    from scmrepo.git import Git

    from dvc.repo import Repo
    from dvc.repo.experiments import Experiments

logger = logging.getLogger(__name__)


@dataclass
class QueueEntry:
    dvc_root: str
    scm_root: str
    stash_ref: str
    stash_rev: str
    baseline_rev: str
    branch: Optional[str]
    name: Optional[str]

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


class BaseStashQueue(ABC):
    """Naive Git-stash based experiment queue.

    Maps queued experiments to (Git) stash reflog entries.
    """

    def __init__(self, repo: "Repo", ref: str):
        """Construct a queue.

        Arguments:
            scm: Git SCM instance for this queue.
            ref: Git stash ref for this queue.
        """
        self.repo = repo
        self.ref = ref

    @property
    def scm(self) -> "Git":
        return self.repo.scm

    @cached_property
    def stash(self) -> ExpStash:
        return ExpStash(self.scm, self.ref)

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

    def remove(self, revs: Collection[str]) -> List[str]:
        """Remove the specified entries from the queue.

        Arguments:
            revs: Stash revisions or queued exp names to be removed.

        Returns:
            Revisions (or names) which were removed.
        """
        to_remove = {}
        removed: List[str] = []
        for stash_rev, stash_entry in self.stash.stash_revs.items():
            if stash_rev in revs:
                to_remove[stash_rev] = stash_entry
                removed.append(stash_rev)
            elif stash_entry.name in revs:
                to_remove[stash_rev] = stash_entry
                removed.append(stash_entry.name)
        self._remove_revs(to_remove)
        return removed

    def clear(self) -> List[str]:
        """Remove all entries from the queue.

        Returns:
            Revisions which were removed.
        """
        stash_revs = self.stash.stash_revs
        removed = list(stash_revs)
        self._remove_revs(stash_revs)
        return removed

    @abstractmethod
    def _remove_revs(self, stash_revs: Mapping[str, ExpStashEntry]):
        """Remove the specified entries from the queue by stash revision."""

    @abstractmethod
    def iter_queued(self) -> Generator[QueueEntry, None, None]:
        """Iterate over items in the queue."""

    @abstractmethod
    def iter_active(self) -> Generator[QueueEntry, None, None]:
        """Iterate over items which are being actively processed."""

    @abstractmethod
    def reproduce(self) -> Mapping[str, Mapping[str, str]]:
        """Reproduce queued experiments sequentially."""

    @abstractmethod
    def get_result(self, entry: QueueEntry) -> Optional[ExecutorResult]:
        """Return result of the specified item."""

    def _stash_exp(
        self,
        *args,
        params: Optional[dict] = None,
        resume_rev: Optional[str] = None,
        baseline_rev: Optional[str] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ) -> QueueEntry:
        """Stash changes from the workspace as an experiment.

        Arguments:
            params: Optional dictionary of parameter values to be used.
                Values take priority over any parameters specified in the
                user's workspace.
            resume_rev: Optional checkpoint resume rev.
            baseline_rev: Optional baseline rev for this experiment, defaults
                to the current SCM rev.
            branch: Optional experiment branch name. If specified, the
                experiment will be added to `branch` instead of creating
                a new branch.
            name: Optional experiment name. If specified this will be used as
                the human-readable name in the experiment branch ref. Has no
                effect of branch is specified.
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

    def _stash_msg(
        self,
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

    def _format_new_params_msg(self, new_params, config_path):
        """Format an error message for when new parameters are identified"""
        new_param_count = len(new_params)
        pluralise = "s are" if new_param_count > 1 else " is"
        param_list = ", ".join(new_params)
        return (
            f"{new_param_count} parameter{pluralise} missing "
            f"from '{config_path}': {param_list}"
        )

    def _update_params(self, params: dict):
        """Update experiment params files with the specified values."""
        from dvc.utils.collections import NewParamsFound, merge_params
        from dvc.utils.serialize import MODIFIERS

        logger.debug("Using experiment params '%s'", params)

        for path in params:
            suffix = self.repo.fs.path.suffix(path).lower()
            modify_data = MODIFIERS[suffix]
            with modify_data(path, fs=self.repo.fs) as data:
                try:
                    merge_params(data, params[path], allow_new=False)
                except NewParamsFound as e:
                    msg = self._format_new_params_msg(e.new_params, path)
                    raise MissingParamsError(msg)

        # Force params file changes to be staged in git
        # Otherwise in certain situations the changes to params file may be
        # ignored when we `git stash` them since mtime is used to determine
        # whether the file is dirty
        self.scm.add(list(params.keys()))

    @staticmethod
    @scm_locked
    def setup_executor(
        exp: "Experiments",
        queue_entry: QueueEntry,
        executor_cls: Type[BaseExecutor] = WorkspaceExecutor,
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

        scm.set_ref(EXEC_HEAD, stash_entry.head_rev)
        scm.set_ref(EXEC_MERGE, stash_rev)
        scm.set_ref(EXEC_BASELINE, stash_entry.baseline_rev)

        # Executor will be initialized with an empty git repo that
        # we populate by pushing:
        #   EXEC_HEAD - the base commit for this experiment
        #   EXEC_MERGE - the unmerged changes (from our stash)
        #       to be reproduced
        #   EXEC_BASELINE - the baseline commit for this experiment
        return executor_cls.from_stash_entry(exp.repo, stash_rev, stash_entry)

    def get_infofile_path(self, name: str) -> str:
        return os.path.join(
            self.pid_dir,
            name,
            f"{name}{BaseExecutor.INFOFILE_EXT}",
        )

    @staticmethod
    @scm_locked
    def collect_executor(
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

        if exec_result.ref_info is not None:
            executor.collect_cache(exp.repo, exec_result.ref_info)

        return results
