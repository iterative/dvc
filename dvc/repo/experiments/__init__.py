import logging
import os
import re
import time
from typing import Dict, Iterable, Optional

from funcy import cached_property, first

from dvc.exceptions import DvcException
from dvc.ui import ui
from dvc.utils import relpath

from .exceptions import (
    BaselineMismatchError,
    ExperimentExistsError,
    InvalidExpRefError,
    MultipleBranchError,
)
from .executor.base import BaseExecutor
from .queue.base import BaseStashQueue, QueueEntry
from .queue.celery import LocalCeleryQueue
from .queue.tempdir import TempDirQueue
from .queue.workspace import WorkspaceQueue
from .refs import (
    CELERY_FAILED_STASH,
    CELERY_STASH,
    EXEC_APPLY,
    EXEC_CHECKPOINT,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    WORKSPACE_STASH,
    ExpRefInfo,
)
from .stash import ExpStashEntry
from .utils import exp_refs_by_rev, scm_locked, unlocked_repo

logger = logging.getLogger(__name__)


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    BRANCH_RE = re.compile(
        r"^(?P<baseline_rev>[a-f0-9]{7})-(?P<exp_sha>[a-f0-9]+)"
        r"(?P<checkpoint>-checkpoint)?$"
    )

    def __init__(self, repo):
        from dvc.lock import make_lock
        from dvc.scm import NoSCMError

        if repo.config["core"].get("no_scm", False):
            raise NoSCMError

        self.repo = repo
        self.scm_lock = make_lock(
            os.path.join(self.repo.tmp_dir, "exp_scm_lock"),
            tmp_dir=self.repo.tmp_dir,
            hardlink_lock=repo.config["core"].get("hardlink_lock", False),
        )

    @property
    def scm(self):
        return self.repo.scm

    @cached_property
    def dvc_dir(self):
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def args_file(self):
        return os.path.join(self.repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)

    @cached_property
    def workspace_queue(self) -> WorkspaceQueue:
        return WorkspaceQueue(self.repo, WORKSPACE_STASH)

    @cached_property
    def tempdir_queue(self) -> TempDirQueue:
        # NOTE: tempdir and workspace stash is shared since both
        # implementations immediately push -> pop (queue length is only 0 or 1)
        return TempDirQueue(self.repo, WORKSPACE_STASH)

    @cached_property
    def celery_queue(self) -> LocalCeleryQueue:
        return LocalCeleryQueue(self.repo, CELERY_STASH, CELERY_FAILED_STASH)

    @property
    def stash_revs(self) -> Dict[str, ExpStashEntry]:
        revs = {}
        for queue in (self.workspace_queue, self.celery_queue):
            revs.update(queue.stash.stash_revs)
        return revs

    def reproduce_one(
        self,
        tmp_dir: bool = False,
        machine: Optional[str] = None,
        **kwargs,
    ):
        """Reproduce and checkout a single (standalone) experiment."""
        if not (tmp_dir or machine):
            staged, _, _ = self.scm.status(untracked_files="no")
            if staged:
                logger.warning(
                    "Your workspace contains staged Git changes which will be "
                    "unstaged before running this experiment."
                )
                self.scm.reset()

        exp_queue: BaseStashQueue = (
            self.tempdir_queue if tmp_dir else self.workspace_queue
        )
        self.queue_one(exp_queue, **kwargs)
        results = self._reproduce_queue(exp_queue)
        exp_rev = first(results)
        if exp_rev is not None:
            self._log_reproduced(results, tmp_dir=tmp_dir)
        return results

    def queue_one(
        self,
        queue: BaseStashQueue,
        checkpoint_resume: Optional[str] = None,
        reset: bool = False,
        **kwargs,
    ) -> QueueEntry:
        """Queue a single experiment."""
        if reset:
            self.reset_checkpoints()

        if kwargs.pop("machine", None) is not None:
            # TODO: decide how to handle queued remote execution
            raise NotImplementedError

        if checkpoint_resume:
            from dvc.scm import resolve_rev

            resume_rev = resolve_rev(self.scm, checkpoint_resume)
            try:
                self.check_baseline(resume_rev)
                checkpoint_resume = resume_rev
            except BaselineMismatchError as exc:
                raise DvcException(
                    f"Cannot resume from '{checkpoint_resume}' as it is not "
                    "derived from your current workspace."
                ) from exc
        else:
            checkpoint_resume = self._workspace_resume_rev()

        return self.new(
            queue,
            checkpoint_resume=checkpoint_resume,
            reset=reset,
            **kwargs,
        )

    def _workspace_resume_rev(self) -> Optional[str]:
        last_checkpoint = self._get_last_checkpoint()
        last_applied = self._get_last_applied()
        if last_checkpoint and last_applied:
            return last_applied
        return None

    def reproduce_celery(
        self, entries: Optional[Iterable[QueueEntry]] = None, **kwargs
    ) -> Dict[str, str]:
        results: Dict[str, str] = {}
        if entries is None:
            entries = list(self.celery_queue.iter_queued())
        if not entries:
            return results

        # TODO: re-enable --jobs concurrency
        self.celery_queue.spawn_worker()
        failed = []
        try:
            ui.write(
                "Following logs for all queued experiments. Use Ctrl+C to "
                "stop following logs (experiment execution will continue).\n"
            )
            for entry in entries:
                # wait for task execution to start
                while not self.celery_queue.proc.get(entry.stash_rev):
                    time.sleep(1)
                self.celery_queue.follow(entry)
                # wait for task collection to complete
                result = self.celery_queue.get_result(entry)
                if result is None or result.exp_hash is None:
                    name = entry.name or entry.stash_rev[:7]
                    failed.append(name)
                elif result.ref_info:
                    exp_rev = self.scm.get_ref(str(result.ref_info))
                    results[exp_rev] = result.exp_hash
        except KeyboardInterrupt:
            ui.write(
                "Experiment(s) are still executing in the background. To "
                "abort execution use 'dvc queue kill' or 'dvc queue stop'."
            )
        if failed:
            names = ", ".join(name for name in failed)
            ui.error(f"Failed to reproduce experiment(s) '{names}'")
        if results:
            self._log_reproduced((rev for rev in results), True)
        return results

    def _log_reproduced(self, revs: Iterable[str], tmp_dir: bool = False):
        names = []
        rev_names = self.get_exact_name(revs)
        for rev in revs:
            name = rev_names[rev]
            names.append(name if name else rev[:7])
        ui.write("\nRan experiment(s): {}".format(", ".join(names)))
        if tmp_dir:
            ui.write(
                "To apply the results of an experiment to your workspace "
                "run:\n\n"
                "\tdvc exp apply <exp>"
            )
        else:
            ui.write("Experiment results have been applied to your workspace.")
        ui.write(
            "\nTo promote an experiment to a Git branch run:\n\n"
            "\tdvc exp branch <exp> <branch>\n"
        )

    def _validate_new_ref(self, exp_ref: ExpRefInfo):
        from .utils import check_ref_format

        if not exp_ref.name:
            return

        check_ref_format(self.scm, exp_ref)

        if self.scm.get_ref(str(exp_ref)):
            raise ExperimentExistsError(exp_ref.name)

    @scm_locked
    def new(
        self,
        queue: BaseStashQueue,
        *args,
        checkpoint_resume: Optional[str] = None,
        **kwargs,
    ) -> QueueEntry:
        """Create and enqueue a new experiment.

        Experiment will be derived from the current workspace.
        """
        if checkpoint_resume is not None:
            return self._resume_checkpoint(
                queue, *args, resume_rev=checkpoint_resume, **kwargs
            )

        name = kwargs.get("name", None)
        baseline_sha = kwargs.get("baseline_rev") or self.repo.scm.get_rev()
        exp_ref = ExpRefInfo(baseline_sha=baseline_sha, name=name)

        try:
            self._validate_new_ref(exp_ref)
        except ExperimentExistsError as err:
            if not (kwargs.get("force", False) or kwargs.get("reset", False)):
                raise err

        return queue.put(*args, **kwargs)

    def _resume_checkpoint(
        self,
        queue: BaseStashQueue,
        *args,
        resume_rev: Optional[str] = None,
        **kwargs,
    ) -> QueueEntry:
        """Create and queue a resumed checkpoint experiment."""
        assert resume_rev

        branch: Optional[str] = None
        try:
            allow_multiple = bool(kwargs.get("params", None))
            branch = self.get_branch_by_rev(
                resume_rev, allow_multiple=allow_multiple
            )
            if not branch:
                raise DvcException(
                    "Could not find checkpoint experiment "
                    f"'{resume_rev[:7]}'"
                )
            baseline_rev = self._get_baseline(branch)
        except MultipleBranchError as exc:
            baselines = {
                info.baseline_sha
                for info in exc.ref_infos
                if info.baseline_sha
            }
            if len(baselines) == 1:
                baseline_rev = baselines.pop()
            else:
                raise

        logger.debug(
            "Checkpoint run from '%s' with baseline '%s'",
            resume_rev[:7],
            baseline_rev,
        )
        return queue.put(
            *args,
            resume_rev=resume_rev,
            baseline_rev=baseline_rev,
            branch=branch,
            **kwargs,
        )

    def _get_last_checkpoint(self) -> Optional[str]:
        try:
            last_checkpoint = self.scm.get_ref(EXEC_CHECKPOINT)
            if last_checkpoint:
                self.check_baseline(last_checkpoint)
            return last_checkpoint
        except BaselineMismatchError:
            # If HEAD has moved since the the last checkpoint run,
            # the specified checkpoint is no longer relevant
            self.scm.remove_ref(EXEC_CHECKPOINT)
        return None

    def _get_last_applied(self) -> Optional[str]:
        try:
            last_applied = self.scm.get_ref(EXEC_APPLY)
            if last_applied:
                self.check_baseline(last_applied)
            return last_applied
        except BaselineMismatchError:
            # If HEAD has moved since the the last applied experiment,
            # the applied experiment is no longer relevant
            self.scm.remove_ref(EXEC_APPLY)
        return None

    def reset_checkpoints(self):
        self.scm.remove_ref(EXEC_CHECKPOINT)
        self.scm.remove_ref(EXEC_APPLY)

    @unlocked_repo
    def _reproduce_queue(
        self, queue: BaseStashQueue, **kwargs
    ) -> Dict[str, str]:
        """Reproduce queued experiments.

        Arguments:
            queue: Experiment queue.

        Returns:
            dict mapping successfully reproduced experiment revs to their
            results.
        """
        exec_results = queue.reproduce()

        results: Dict[str, str] = {}
        for _, exp_result in exec_results.items():
            results.update(exp_result)
        return results

    def check_baseline(self, exp_rev):
        baseline_sha = self.repo.scm.get_rev()
        if exp_rev == baseline_sha:
            return exp_rev

        exp_baseline = self._get_baseline(exp_rev)
        if exp_baseline is None:
            # if we can't tell from branch name, fall back to parent commit
            exp_commit = self.scm.resolve_commit(exp_rev)
            if exp_commit:
                exp_baseline = first(exp_commit.parents)
        if exp_baseline == baseline_sha:
            return exp_baseline
        raise BaselineMismatchError(exp_baseline, baseline_sha)

    def get_baseline(self, rev):
        """Return the baseline rev for an experiment rev."""
        return self._get_baseline(rev)

    def _get_baseline(self, rev):
        from dvc.scm import resolve_rev

        rev = resolve_rev(self.scm, rev)

        if rev in self.stash_revs:
            entry = self.stash_revs.get(rev)
            if entry:
                return entry.baseline_rev
            return None

        ref_info = first(exp_refs_by_rev(self.scm, rev))
        if ref_info:
            return ref_info.baseline_sha
        return None

    def get_branch_by_rev(
        self, rev: str, allow_multiple: bool = False
    ) -> Optional[str]:
        """Returns full refname for the experiment branch containing rev."""
        ref_infos = list(exp_refs_by_rev(self.scm, rev))
        if not ref_infos:
            return None
        if len(ref_infos) > 1 and not allow_multiple:
            for ref_info in ref_infos:
                if self.scm.get_ref(str(ref_info)) == rev:
                    return str(ref_info)
            raise MultipleBranchError(rev, ref_infos)
        return str(ref_infos[0])

    def get_exact_name(self, revs: Iterable[str]) -> Dict[str, Optional[str]]:
        """Returns preferred name for the specified revision.

        Prefers tags, branches (heads), experiments in that orer.
        """
        result: Dict[str, Optional[str]] = {}
        exclude = f"{EXEC_NAMESPACE}/*"
        ref_dict = self.scm.describe(
            revs, base=EXPS_NAMESPACE, exclude=exclude
        )
        for rev in revs:
            name: Optional[str] = None
            ref = ref_dict[rev]
            if ref:
                try:
                    name = ExpRefInfo.from_ref(ref).name
                except InvalidExpRefError:
                    pass
            if not name:
                if rev in self.stash_revs:
                    name = self.stash_revs[rev].name
                elif rev in self.celery_queue.failed_stash.stash_revs:
                    name = self.celery_queue.failed_stash.stash_revs[rev].name
            result[rev] = name
        return result

    def get_running_exps(self, fetch_refs: bool = True) -> Dict[str, Dict]:
        """Return info for running experiments."""
        result = {}
        for queue in (
            self.workspace_queue,
            self.tempdir_queue,
            self.celery_queue,
        ):
            result.update(queue.get_running_exps(fetch_refs))
        return result

    def apply(self, *args, **kwargs):
        from dvc.repo.experiments.apply import apply

        return apply(self.repo, *args, **kwargs)

    def branch(self, *args, **kwargs):
        from dvc.repo.experiments.branch import branch

        return branch(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from dvc.repo.experiments.diff import diff

        return diff(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)

    def run(self, *args, **kwargs):
        from dvc.repo.experiments.run import run

        return run(self.repo, *args, **kwargs)

    def gc(self, *args, **kwargs):
        from dvc.repo.experiments.gc import gc

        return gc(self.repo, *args, **kwargs)

    def push(self, *args, **kwargs):
        from dvc.repo.experiments.push import push

        return push(self.repo, *args, **kwargs)

    def pull(self, *args, **kwargs):
        from dvc.repo.experiments.pull import pull

        return pull(self.repo, *args, **kwargs)

    def ls(self, *args, **kwargs):
        from dvc.repo.experiments.ls import ls

        return ls(self.repo, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from dvc.repo.experiments.remove import remove

        return remove(self.repo, *args, **kwargs)
