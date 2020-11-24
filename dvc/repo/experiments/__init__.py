import logging
import os
import re
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from functools import wraps
from typing import Iterable, Optional

from funcy import cached_property, first

from dvc.exceptions import DvcException
from dvc.path_info import PathInfo
from dvc.repo.experiments.base import (
    EXEC_HEAD,
    EXEC_MERGE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    get_exps_refname,
    split_exps_refname,
)
from dvc.repo.experiments.executor import BaseExecutor, LocalExecutor
from dvc.stage.run import CheckpointKilledError
from dvc.utils import env2bool, relpath

logger = logging.getLogger(__name__)


def scm_locked(f):
    # Lock the experiments workspace so that we don't try to perform two
    # different sequences of git operations at once
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        with exp.scm_lock:
            return f(exp, *args, **kwargs)

    return wrapper


class BaselineMismatchError(DvcException):
    def __init__(self, rev, expected):
        if hasattr(rev, "hexsha"):
            rev = rev.hexsha
        rev_str = f"{rev[:7]}" if rev is not None else "dangling commit"
        super().__init__(
            f"Experiment derived from '{rev_str}', expected '{expected[:7]}'."
        )
        self.rev = rev
        self.expected_rev = expected


class CheckpointExistsError(DvcException):
    def __init__(self, rev, continue_rev):
        msg = (
            f"Checkpoint experiment containing '{rev[:7]}' already exists."
            " To restart the experiment run:\n\n"
            "\tdvc exp run -f ...\n\n"
            "To resume the experiment, run:\n\n"
            f"\tdvc exp resume {continue_rev[:7]}\n"
        )
        super().__init__(msg)
        self.rev = rev


class MultipleBranchError(DvcException):
    def __init__(self, rev):
        super().__init__(
            f"Ambiguous commit '{rev[:7]}' belongs to multiple experiment "
            "branches."
        )
        self.rev = rev


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    EXPERIMENTS_DIR = "experiments"
    STASH_EXPERIMENT_FORMAT = "dvc-exp:{rev}:{baseline_rev}"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:commit: )"
        r"dvc-exp:(?P<rev>[0-9a-f]+):(?P<baseline_rev>[0-9a-f]+)"
        r"(:(?P<branch>.+))?$"
    )
    BRANCH_RE = re.compile(
        r"^(?P<baseline_rev>[a-f0-9]{7})-(?P<exp_sha>[a-f0-9]+)"
        r"(?P<checkpoint>-checkpoint)?$"
    )
    LAST_CHECKPOINT = ":last"

    StashEntry = namedtuple(
        "StashEntry", ["index", "rev", "baseline_rev", "branch"]
    )

    def __init__(self, repo):
        from dvc.lock import make_lock

        if not (
            env2bool("DVC_TEST")
            or repo.config["core"].get("experiments", False)
        ):
            raise NotImplementedError

        self.repo = repo
        self.scm_lock = make_lock(
            os.path.join(self.repo.tmp_dir, "exp_scm_lock"),
            tmp_dir=self.repo.tmp_dir,
        )

    @cached_property
    def exp_dir(self):
        return os.path.join(self.repo.dvc_dir, self.EXPERIMENTS_DIR)

    @property
    def scm(self):
        return self.repo.scm

    @cached_property
    def dvc_dir(self):
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def exp_dvc_dir(self):
        return os.path.join(self.exp_dir, self.dvc_dir)

    @property
    def exp_dvc(self):
        return self.repo

    @contextmanager
    def chdir(self):
        yield

    @cached_property
    def args_file(self):
        return os.path.join(
            self.exp_dvc.tmp_dir, BaseExecutor.PACKED_ARGS_FILE
        )

    @cached_property
    def stash(self):
        from dvc.scm.git import Stash

        return Stash(self.scm, EXPS_STASH)

    @property
    def stash_revs(self):
        revs = {}
        for i, entry in enumerate(self.stash):
            msg = entry.message.decode("utf-8").strip()
            m = self.STASH_EXPERIMENT_RE.match(msg)
            if m:
                revs[entry.new_sha.decode("utf-8")] = self.StashEntry(
                    i,
                    m.group("rev"),
                    m.group("baseline_rev"),
                    m.group("branch"),
                )
        return revs

    def get_refname(self, baseline: str, name: Optional[str] = None):
        """Return git ref name for the specified experiment.

        Args:
            baseline: baseline git commit SHA (or named ref)
            name: experiment name
        """
        return get_exps_refname(self.scm, baseline, name=name)

    def _scm_checkout(self, rev, **kwargs):
        self.scm.repo.git.reset(hard=True)
        self.scm.repo.git.clean(force=True)
        logger.debug("Checking out experiment commit '%s'", rev)
        self.scm.checkout(rev, **kwargs)

    def _checkout_default_branch(self):
        from git.refs.symbolic import SymbolicReference

        # switch to default branch
        git_repo = self.scm.repo
        git_repo.git.reset(hard=True)
        git_repo.git.clean(force=True)
        origin_refs = git_repo.remotes["origin"].refs

        # origin/HEAD will point to tip of the default branch unless we
        # initially cloned a repo that was in a detached-HEAD state.
        #
        # If we are currently detached because we cloned a detached
        # repo, we can't actually tell what branch should be considered
        # default, so we just fall back to the first available reference.
        if "HEAD" in origin_refs:
            ref = origin_refs["HEAD"].reference
        else:
            ref = origin_refs[0]
            if not isinstance(ref, SymbolicReference):
                ref = ref.reference
        branch_name = ref.name.split("/")[-1]

        if branch_name in git_repo.heads:
            branch = git_repo.heads[branch_name]
        else:
            branch = git_repo.create_head(branch_name, ref)
            branch.set_tracking_branch(ref)
        branch.checkout()

    def _stash_exp(
        self,
        *args,
        params: Optional[dict] = None,
        baseline_rev: Optional[str] = None,
        branch: Optional[str] = None,
        **kwargs,
    ):
        """Stash changes from the workspace as an experiment.

        Args:
            params: Optional dictionary of parameter values to be used.
                Values take priority over any parameters specified in the
                user's workspace.
            baseline_rev: Optional baseline rev for this experiment, defaults
                to the current SCM rev.
            branch: Optional experiment branch name. If specified, the
                experiment will be added to `branch` instead of creating
                a new branch.
        """
        with self.scm.stash_workspace(include_untracked=True) as workspace:
            # If we are not extending an existing branch, apply current
            # workspace changes to be made in new branch
            if not branch:
                self.stash.apply(workspace)

            # checkout and detach at branch (or current HEAD)
            with self.scm.detach_head(branch) as rev:
                if baseline_rev is None:
                    baseline_rev = rev

                # update experiment params from command line
                if params:
                    self._update_params(params)

                # save additional repro command line arguments
                self._pack_args(*args, **kwargs)

                # save experiment as a stash commit
                msg = self._stash_msg(
                    rev, baseline_rev=baseline_rev, branch=branch
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

            # Reset/clean any changes before prior workspace is unstashed
            self.scm.repo.git.reset(hard=True)
            self.scm.repo.git.clean(force=True)

        return stash_rev

    def _stash_msg(self, rev: str, baseline_rev: str, branch=None):
        if not baseline_rev:
            baseline_rev = rev
        msg = self.STASH_EXPERIMENT_FORMAT.format(
            rev=rev, baseline_rev=baseline_rev
        )
        if branch:
            return f"{msg}:{branch}"
        return msg

    def _pack_args(self, *args, **kwargs):
        BaseExecutor.pack_repro_args(self.args_file, *args, **kwargs)
        self.scm.add(self.args_file)

    def _update_params(self, params: dict):
        """Update experiment params files with the specified values."""
        from benedict import benedict

        from dvc.utils.serialize import MODIFIERS

        logger.debug("Using experiment params '%s'", params)

        for params_fname in params:
            path = PathInfo(self.exp_dvc.root_dir) / params_fname
            suffix = path.suffix.lower()
            modify_data = MODIFIERS[suffix]
            with modify_data(path, tree=self.exp_dvc.tree) as data:
                benedict(data).merge(params[params_fname], overwrite=True)

        # Force params file changes to be staged in git
        # Otherwise in certain situations the changes to params file may be
        # ignored when we `git stash` them since mtime is used to determine
        # whether the file is dirty
        self.scm.add(list(params.keys()))

    def _reset_checkpoint_branch(self, branch, rev, branch_tip, reset):
        if not reset:
            raise CheckpointExistsError(rev, branch_tip)
        self._checkout_default_branch()
        logger.debug("Removing existing checkpoint branch '%s'", branch)
        self.scm.repo.git.branch(branch, D=True)

    def reproduce_one(self, queue=False, **kwargs):
        """Reproduce and checkout a single experiment."""
        stash_rev = self.new(**kwargs)
        if queue:
            logger.info(
                "Queued experiment '%s' for future execution.", stash_rev[:7]
            )
            return [stash_rev]
        results = self.reproduce([stash_rev], keep_stash=False)
        # exp_rev = first(results)
        # if exp_rev is not None:
        #     self.checkout_exp(exp_rev)
        return results

    def reproduce_queued(self, **kwargs):
        results = self.reproduce(**kwargs)
        if results:
            revs = [f"{rev[:7]}" for rev in results]
            logger.info(
                "Successfully reproduced experiment(s) '%s'.\n"
                "Use `dvc exp checkout <exp_rev>` to apply the results of "
                "a specific experiment to your workspace.",
                ", ".join(revs),
            )
        return results

    @scm_locked
    def new(
        self, *args, checkpoint_resume: Optional[str] = None, **kwargs,
    ):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        if checkpoint_resume is not None:
            return self._resume_checkpoint(
                *args, checkpoint_resume=checkpoint_resume, **kwargs
            )

        return self._stash_exp(*args, **kwargs)

    def _resume_checkpoint(
        self, *args, checkpoint_resume: Optional[str] = None, **kwargs,
    ):
        """Resume an existing (checkpoint) experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        assert checkpoint_resume

        branch = None
        if checkpoint_resume == self.LAST_CHECKPOINT:
            # Continue from most recently committed checkpoint
            branch = self._get_last_checkpoint()
            resume_rev = self.scm.resolve_rev(branch)
        else:
            rev = self.scm.resolve_rev(checkpoint_resume)
            resume_rev = rev
            branch = self._get_branch_containing(rev)
            if not branch:
                raise DvcException(
                    "Could not find checkpoint experiment "
                    f"'{checkpoint_resume}'"
                )

        baseline_rev = self._get_baseline(branch)
        if kwargs.get("params", None):
            logger.debug(
                "Branching from checkpoint '%s' with modified params",
                checkpoint_resume,
            )
            rev = resume_rev
            branch = None
        else:
            logger.debug(
                "Continuing checkpoint experiment '%s'", checkpoint_resume
            )
            rev = self.scm.resolve_rev(branch)
            logger.debug(
                "Using '%s' (tip of branch '%s') as baseline", rev, branch
            )
        self._scm_checkout(rev)

        kwargs["apply_workspace"] = False
        stash_rev = self._stash_exp(
            *args, baseline_rev=baseline_rev, branch=branch, **kwargs
        )
        logger.debug(
            "Stashed experiment '%s' for future execution.", stash_rev[:7]
        )
        return stash_rev

    def _get_last_checkpoint(self):
        for head in sorted(
            self.scm.repo.heads,
            key=lambda h: h.commit.committed_date,
            reverse=True,
        ):
            exp_branch = head.name
            m = self.BRANCH_RE.match(exp_branch)
            if m and m.group("checkpoint"):
                return exp_branch
        raise DvcException("No existing checkpoint experiment to continue")

    @scm_locked
    def reproduce(
        self,
        revs: Optional[Iterable] = None,
        keep_stash: Optional[bool] = True,
        **kwargs,
    ):
        """Reproduce the specified experiments.

        Args:
            revs: If revs is not specified, all stashed experiments will be
                reproduced.
            keep_stash: If True, stashed experiments will be preserved if they
                fail to reproduce successfully.
        """
        stash_revs = self.stash_revs

        # to_run contains mapping of:
        #   input_rev: (stash_index, rev, baseline_rev)
        # where input_rev contains the changes to execute (usually a stash
        # commit), rev is the original SCM commit to be checked out, and
        # baseline_rev is the experiment baseline.
        if revs is None:
            to_run = dict(stash_revs)
        else:
            to_run = {
                rev: stash_revs[rev]
                if rev in stash_revs
                else self.StashEntry(None, rev, rev, None)
                for rev in revs
            }

        logger.debug(
            "Reproducing experiment revs '%s'",
            ", ".join((rev[:7] for rev in to_run)),
        )

        # setup executors - unstash experiment, generate executor, upload
        # contents of (unstashed) exp workspace to the executor tree
        executors = {}
        with self.scm.stash_workspace(include_untracked=True):
            with self.scm.detach_head():
                for stash_rev, item in to_run.items():
                    self.scm.set_ref(EXEC_HEAD, item.rev)
                    self.scm.set_ref(EXEC_MERGE, stash_rev)

                    # Executor will be initialized with an empty git repo that
                    # we populate by pushing:
                    #   1. EXEC_HEAD - the base commit for this experiment
                    #   2. EXEC_MERGE - the unmerged changes (from our stash)
                    #       to be reproduced
                    #   3. the existing experiment branch (if it exists)
                    executor = LocalExecutor(
                        self.scm,
                        self.dvc_dir,
                        branch=item.branch,
                        cache_dir=self.repo.cache.local.cache_dir,
                    )

                    executors[item.rev] = executor

            self.scm.repo.git.reset(hard=True)
            self.scm.repo.git.clean(force=True)

        exec_results = self._reproduce(executors, **kwargs)

        if keep_stash:
            # only drop successfully run stashed experiments
            to_drop = sorted(
                (
                    stash_revs[rev][0]
                    for rev in exec_results
                    if rev in stash_revs
                ),
                reverse=True,
            )
        else:
            # drop all stashed experiments
            to_drop = sorted(
                (stash_revs[rev][0] for rev in to_run if rev in stash_revs),
                reverse=True,
            )
        for index in to_drop:
            self.stash.drop(index)

        result = {}
        for _, exp_result in exec_results.items():
            result.update(exp_result)
        return result

    def _reproduce(self, executors: dict, jobs: Optional[int] = 1) -> dict:
        """Run dvc repro for the specified BaseExecutors in parallel.

        Returns dict containing successfully executed experiments.
        """
        from multiprocessing import get_context

        result: dict = {}

        with ProcessPoolExecutor(
            max_workers=jobs, mp_context=get_context("spawn")
        ) as workers:
            futures = {}
            for rev, executor in executors.items():
                future = workers.submit(executor.reproduce, executor.dvc_dir,)
                futures[future] = (rev, executor)

            for future in as_completed(futures):
                rev, executor = futures[future]
                exc = future.exception()

                try:
                    if exc is None:
                        exp_hash = future.result()
                        self._collect_executor(executor, rev, exp_hash, result)
                    else:
                        # Checkpoint errors have already been logged
                        if not isinstance(exc, CheckpointKilledError):
                            logger.exception(
                                "Failed to reproduce experiment '%s'",
                                rev[:7],
                                exc_info=exc,
                            )
                finally:
                    executor.cleanup()

        return result

    def _collect_executor(self, executor, rev, exp_hash, result):
        # NOTE: GitPython Repo instances cannot be re-used
        # after process has received SIGINT or SIGTERM, so we
        # need this hack to re-instantiate git instances after
        # checkpoint runs. See:
        # https://github.com/gitpython-developers/GitPython/issues/427
        del self.repo.scm

        for ref in executor.fetch_exps(self.scm):
            exp_rev = self.scm.get_ref(ref)
            if exp_rev:
                logger.info("Reproduced experiment '%s'.", exp_rev[:7])
                result[rev] = {exp_rev: exp_hash}

    @scm_locked
    def checkout_exp(self, rev, **kwargs):
        """Checkout an experiment to the user's workspace."""
        from dvc.repo.checkout import checkout as dvc_checkout

        self._check_baseline(rev)
        branch = self._get_branch_containing(rev)

        with self.scm.stash_workspace(include_untracked=True):
            self.scm.repo.git.merge(branch, squash=True, no_commit=True)
            self.scm.repo.git.reset()

        dvc_checkout(self.repo, **kwargs)

    def _check_baseline(self, exp_rev):
        baseline_sha = self.repo.scm.get_rev()
        if exp_rev == baseline_sha:
            return exp_rev

        exp_baseline = self._get_baseline(exp_rev)
        if exp_baseline is None:
            # if we can't tell from branch name, fall back to parent commit
            exp_commit = self.scm.repo.rev_parse(exp_rev)
            exp_baseline = first(exp_commit.parents).hexsha
        if exp_baseline == baseline_sha:
            return exp_baseline
        raise BaselineMismatchError(exp_baseline, baseline_sha)

    @scm_locked
    def get_baseline(self, rev):
        """Return the baseline rev for an experiment rev."""
        return self._get_baseline(rev)

    def _get_baseline(self, rev):
        ref = first(self.scm.get_refs_containing(rev, EXPS_NAMESPACE))
        if not ref:
            return None
        if ref == EXPS_STASH:
            entry = self.stash_revs.get(rev)
            if entry:
                return entry.baseline_rev
            return None
        try:
            _, sha, _ = split_exps_refname(ref)
            return sha
        except ValueError:
            return None

    def _get_branch_containing(self, rev):
        names = [
            ref
            for ref in self.scm.get_refs_containing(rev, EXPS_NAMESPACE)
            if ref != EXPS_STASH
        ]
        if not names:
            return None
        if len(names) > 1:
            raise MultipleBranchError(rev)
        return names[0]

    def checkout(self, *args, **kwargs):
        from dvc.repo.experiments.checkout import checkout

        return checkout(self.repo, *args, **kwargs)

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
