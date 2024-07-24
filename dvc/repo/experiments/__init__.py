import os
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

from funcy import chain, first

from dvc.log import logger
from dvc.ui import ui
from dvc.utils import relpath
from dvc.utils.objects import cached_property

from .cache import ExpCache
from .exceptions import (
    BaselineMismatchError,
    ExperimentExistsError,
    InvalidExpRefError,
    MultipleBranchError,
)
from .refs import (
    APPLY_STASH,
    CELERY_FAILED_STASH,
    CELERY_STASH,
    EXEC_APPLY,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    WORKSPACE_STASH,
    ExpRefInfo,
)
from .stash import ApplyStash
from .utils import check_ref_format, exp_refs_by_rev, unlocked_repo

if TYPE_CHECKING:
    from .queue.base import BaseStashQueue, QueueEntry
    from .queue.celery import LocalCeleryQueue
    from .queue.tempdir import TempDirQueue
    from .queue.workspace import WorkspaceQueue
    from .stash import ExpStashEntry

logger = logger.getChild(__name__)


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    BRANCH_RE = re.compile(r"^(?P<baseline_rev>[a-f0-9]{7})-(?P<exp_sha>[a-f0-9]+)")

    def __init__(self, repo):
        from dvc.scm import NoSCMError

        if repo.config["core"].get("no_scm", False):
            raise NoSCMError

        self.repo = repo

    @property
    def scm(self):
        from dvc.scm import SCMError

        if self.repo.scm.no_commits:
            raise SCMError("Empty Git repo. Add a commit to use experiments.")

        return self.repo.scm

    @cached_property
    def dvc_dir(self) -> str:
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def args_file(self) -> str:
        from .executor.base import BaseExecutor

        return os.path.join(self.repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)

    @cached_property
    def workspace_queue(self) -> "WorkspaceQueue":
        from .queue.workspace import WorkspaceQueue

        return WorkspaceQueue(self.repo, WORKSPACE_STASH)

    @cached_property
    def tempdir_queue(self) -> "TempDirQueue":
        from .queue.tempdir import TempDirQueue

        # NOTE: tempdir and workspace stash is shared since both
        # implementations immediately push -> pop (queue length is only 0 or 1)
        return TempDirQueue(self.repo, WORKSPACE_STASH)

    @cached_property
    def celery_queue(self) -> "LocalCeleryQueue":
        from .queue.celery import LocalCeleryQueue

        return LocalCeleryQueue(self.repo, CELERY_STASH, CELERY_FAILED_STASH)

    @cached_property
    def apply_stash(self) -> ApplyStash:
        return ApplyStash(self.scm, APPLY_STASH)

    @cached_property
    def cache(self) -> ExpCache:
        return ExpCache(self.repo)

    @property
    def stash_revs(self) -> dict[str, "ExpStashEntry"]:
        revs = {}
        for queue in (self.workspace_queue, self.celery_queue):
            revs.update(queue.stash.stash_revs)
        return revs

    def reproduce_one(
        self,
        tmp_dir: bool = False,
        copy_paths: Optional[list[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ):
        """Reproduce and checkout a single (standalone) experiment."""
        exp_queue: BaseStashQueue = (
            self.tempdir_queue if tmp_dir else self.workspace_queue
        )
        self.queue_one(exp_queue, **kwargs)
        results = self._reproduce_queue(
            exp_queue, copy_paths=copy_paths, message=message
        )
        exp_rev = first(results)
        if exp_rev is not None:
            self._log_reproduced(results, tmp_dir=tmp_dir)
        return results

    def queue_one(self, queue: "BaseStashQueue", **kwargs) -> "QueueEntry":
        """Queue a single experiment."""
        return self.new(queue, **kwargs)

    def reproduce_celery(
        self, entries: Optional[Iterable["QueueEntry"]] = None, **kwargs
    ) -> dict[str, str]:
        results: dict[str, str] = {}
        if entries is None:
            entries = list(
                chain(self.celery_queue.iter_active(), self.celery_queue.iter_queued())
            )

        logger.debug("reproduce all these entries '%s'", entries)

        if not entries:
            return results

        self.celery_queue.start_workers(count=kwargs.get("jobs", 1))
        failed = []
        try:
            ui.write(
                "Following logs for all queued experiments. Use Ctrl+C to "
                "stop following logs (experiment execution will continue).\n"
            )
            for entry in entries:
                # wait for task execution to start
                self.celery_queue.wait_for_start(entry, sleep_interval=1)
                self.celery_queue.follow(entry)
                # wait for task collection to complete
                try:
                    result = self.celery_queue.get_result(entry)
                except FileNotFoundError:
                    result = None
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

    def new(self, queue: "BaseStashQueue", *args, **kwargs) -> "QueueEntry":
        """Create and enqueue a new experiment.

        Experiment will be derived from the current workspace.
        """

        name = kwargs.get("name", None)
        baseline_sha = kwargs.get("baseline_rev") or self.repo.scm.get_rev()

        if name:
            exp_ref = ExpRefInfo(baseline_sha=baseline_sha, name=name)
            check_ref_format(self.scm, exp_ref)
            force = kwargs.get("force", False)
            if self.scm.get_ref(str(exp_ref)) and not force:
                raise ExperimentExistsError(exp_ref.name)

        return queue.put(*args, **kwargs)

    def _get_last_applied(self) -> Optional[str]:
        try:
            last_applied = self.scm.get_ref(EXEC_APPLY)
            if last_applied:
                self.check_baseline(last_applied)
            return last_applied
        except BaselineMismatchError:
            # If HEAD has moved since the last applied experiment,
            # the applied experiment is no longer relevant
            self.scm.remove_ref(EXEC_APPLY)
        return None

    @unlocked_repo
    def _reproduce_queue(
        self,
        queue: "BaseStashQueue",
        copy_paths: Optional[list[str]] = None,
        message: Optional[str] = None,
        **kwargs,
    ) -> dict[str, str]:
        """Reproduce queued experiments.

        Arguments:
            queue: Experiment queue.

        Returns:
            dict mapping successfully reproduced experiment revs to their
            results.
        """
        exec_results = queue.reproduce(copy_paths=copy_paths, message=message)

        results: dict[str, str] = {}
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

    def get_exact_name(self, revs: Iterable[str]) -> dict[str, Optional[str]]:
        """Returns preferred name for the specified revision.

        Prefers tags, branches (heads), experiments in that order.
        """
        result: dict[str, Optional[str]] = {}
        exclude = f"{EXEC_NAMESPACE}/*"
        ref_dict = self.scm.describe(revs, base=EXPS_NAMESPACE, exclude=exclude)
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
                else:
                    failed_stash = self.celery_queue.failed_stash
                    if failed_stash and rev in failed_stash.stash_revs:
                        name = failed_stash.stash_revs[rev].name
            result[rev] = name
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

    def save(self, *args, **kwargs):
        from dvc.repo.experiments.save import save

        return save(self.repo, *args, **kwargs)

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

    def rename(self, *args, **kwargs):
        from dvc.repo.experiments.rename import rename

        return rename(self.repo, *args, **kwargs)

    def clean(self, *args, **kwargs):
        from dvc.repo.experiments.clean import clean

        return clean(self.repo, *args, **kwargs)
