import logging
import os
import re
import stat
import tempfile
import threading
from collections import namedtuple
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from functools import partial, wraps
from typing import Iterable, Optional

from funcy import cached_property, first

from dvc.dvcfile import is_lock_file
from dvc.exceptions import DownloadError, DvcException, UploadError
from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.repo.experiments.executor import ExperimentExecutor, LocalExecutor
from dvc.scm.git import Git
from dvc.stage import PipelineStage
from dvc.stage.run import CheckpointKilledError
from dvc.stage.serialize import to_lockfile
from dvc.tree.repo import RepoTree
from dvc.utils import dict_sha256, env2bool, relpath
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


def scm_locked(f):
    # Lock the experiments workspace so that we don't try to perform two
    # different sequences of git operations at once
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        with exp.scm_lock:
            return f(exp, *args, **kwargs)

    return wrapper


def hash_exp(stages):
    exp_data = {}
    for stage in stages:
        if isinstance(stage, PipelineStage):
            exp_data.update(to_lockfile(stage))
    return dict_sha256(exp_data)


class UnchangedExperimentError(DvcException):
    def __init__(self, rev):
        super().__init__(f"Experiment identical to baseline '{rev[:7]}'.")
        self.rev = rev


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
    PACKED_ARGS_FILE = "repro.dat"
    STASH_EXPERIMENT_FORMAT = "dvc-exp:{rev}:{baseline_rev}"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:On \(.*\): )"
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

    @cached_property
    def scm(self):
        """Experiments clone scm instance."""
        if os.path.exists(self.exp_dir):
            return Git(self.exp_dir)
        return self._init_clone()

    @cached_property
    def dvc_dir(self):
        return relpath(self.repo.dvc_dir, self.repo.scm.root_dir)

    @cached_property
    def exp_dvc_dir(self):
        return os.path.join(self.exp_dir, self.dvc_dir)

    @cached_property
    def exp_dvc(self):
        """Return clone dvc Repo instance."""
        from dvc.repo import Repo

        return Repo(self.exp_dvc_dir)

    @contextmanager
    def chdir(self):
        cwd = os.getcwd()
        os.chdir(self.exp_dvc.root_dir)
        yield self.exp_dvc.root_dir
        os.chdir(cwd)

    @cached_property
    def args_file(self):
        return os.path.join(self.exp_dvc.tmp_dir, self.PACKED_ARGS_FILE)

    @property
    def stash_reflog(self):
        """Return stash reflog in Git 'stash@{...}' index order."""
        if "refs/stash" in self.scm.repo.refs:
            # gitpython reflog log() returns commits oldest to newest
            return reversed(self.scm.repo.refs["refs/stash"].log())
        return []

    @property
    def stash_revs(self):
        revs = {}
        for i, entry in enumerate(self.stash_reflog):
            m = self.STASH_EXPERIMENT_RE.match(entry.message)
            if m:
                revs[entry.newhexsha] = self.StashEntry(
                    i,
                    m.group("rev"),
                    m.group("baseline_rev"),
                    m.group("branch"),
                )
        return revs

    def _init_clone(self):
        src_dir = self.repo.scm.root_dir
        logger.debug("Initializing experiments clone")
        git = Git.clone(src_dir, self.exp_dir)
        self._config_clone()
        return git

    def _config_clone(self):
        dvc_dir = relpath(self.repo.dvc_dir, self.repo.scm.root_dir)
        local_config = os.path.join(self.exp_dir, dvc_dir, "config.local")
        cache_dir = self.repo.cache.local.cache_dir
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    def _scm_checkout(self, rev):
        self.scm.repo.git.reset(hard=True)
        self.scm.repo.git.clean(force=True)
        if self.scm.repo.head.is_detached:
            self._checkout_default_branch()
        if not self.scm.has_rev(rev):
            self.scm.pull()
        logger.debug("Checking out experiment commit '%s'", rev)
        self.scm.checkout(rev)

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
        allow_unchanged: Optional[bool] = True,
        apply_workspace: Optional[bool] = True,
        **kwargs,
    ):
        """Stash changes from the current (parent) workspace as an experiment.

        Args:
            params: Optional dictionary of parameter values to be used.
                Values take priority over any parameters specified in the
                user's workspace.
            baseline_rev: Optional baseline rev for this experiment, defaults
                to the current SCM rev.
            branch: Optional experiment branch name. If specified, the
                experiment will be added to `branch` instead of creating
                a new branch.
            allow_unchanged: Force experiment reproduction even if params are
                unchanged from the baseline.
            apply_workspace: Apply changes from the user workspace to the
                experiment workspace.
        """
        if branch:
            rev = self.scm.resolve_rev(branch)
        else:
            rev = self.scm.get_rev()

        if apply_workspace:
            if branch:
                raise DvcException(
                    "Cannot apply workspace changes into existing experiment"
                    "branch."
                )
            # patch user's workspace into experiments clone
            tmp = tempfile.NamedTemporaryFile(delete=False).name
            try:
                self.repo.scm.repo.git.diff(
                    patch=True, full_index=True, binary=True, output=tmp
                )
                if os.path.getsize(tmp):
                    logger.debug("Patching experiment workspace")
                    self.scm.repo.git.apply(tmp)
            finally:
                remove(tmp)

        # update experiment params from command line
        if params:
            self._update_params(params)

        if not self._check_dirty() and not allow_unchanged:
            # experiment matches original baseline
            raise UnchangedExperimentError(rev)

        # save additional repro command line arguments
        self._pack_args(*args, **kwargs)

        # save experiment as a stash commit w/message containing baseline rev
        # (stash commits are merge commits and do not contain a parent commit
        # SHA)
        msg = self._stash_msg(rev, baseline_rev=baseline_rev, branch=branch)
        self.scm.repo.git.stash("push", "-m", msg)
        return self.scm.resolve_rev("stash@{0}")

    def _check_dirty(self) -> bool:
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

    def _stash_msg(self, rev, baseline_rev=None, branch=None):
        if not baseline_rev:
            baseline_rev = rev
        msg = self.STASH_EXPERIMENT_FORMAT.format(
            rev=rev, baseline_rev=baseline_rev
        )
        if branch:
            return f"{msg}:{branch}"
        return msg

    def _pack_args(self, *args, **kwargs):
        ExperimentExecutor.pack_repro_args(self.args_file, *args, **kwargs)
        self.scm.add(self.args_file)

    def _unpack_args(self, tree=None):
        return ExperimentExecutor.unpack_repro_args(self.args_file, tree=tree)

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

    def _commit(
        self,
        exp_hash: str,
        baseline_rev: Optional[str] = None,
        check_exists: Optional[bool] = True,
        create_branch: Optional[bool] = True,
        checkpoint: Optional[bool] = False,
        checkpoint_reset: Optional[bool] = False,
    ):
        """Commit stages as an experiment and return the commit SHA."""
        if not self.scm.is_dirty(untracked_files=True):
            raise UnchangedExperimentError(self.scm.get_rev())

        rev = self.scm.get_rev()
        if not baseline_rev:
            baseline_rev = rev
        checkpoint = "-checkpoint" if checkpoint else ""
        exp_name = f"{baseline_rev[:7]}-{exp_hash}{checkpoint}"
        if create_branch:
            if (
                check_exists or checkpoint
            ) and exp_name in self.scm.list_branches():
                branch_tip = self.scm.resolve_rev(exp_name)
                if checkpoint:
                    self._reset_checkpoint_branch(
                        exp_name, rev, branch_tip, checkpoint_reset
                    )
                else:
                    logger.debug(
                        "Using existing experiment branch '%s'", exp_name
                    )
                    return branch_tip
            self.scm.checkout(exp_name, create_new=True)
            logger.debug("Commit new experiment branch '%s'", exp_name)
        else:
            logger.debug("Commit to current experiment branch")
        self.scm.repo.git.add(A=True)
        self.scm.commit(f"Add experiment {exp_name}")
        return self.scm.get_rev()

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
        exp_rev = first(results)
        if exp_rev is not None:
            self.checkout_exp(exp_rev)
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
        self, *args, branch: Optional[str] = None, **kwargs,
    ):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        if kwargs.get("checkpoint_resume", None) is not None:
            return self._resume_checkpoint(*args, **kwargs)

        if branch:
            rev = self.scm.resolve_rev(branch)
            logger.debug(
                "Using '%s' (tip of branch '%s') as baseline", rev, branch
            )
        else:
            rev = self.repo.scm.get_rev()
        self._scm_checkout(rev)

        force = kwargs.get("force", False)
        try:
            stash_rev = self._stash_exp(
                *args, branch=branch, checkpoint_reset=force, **kwargs,
            )
        except UnchangedExperimentError as exc:
            logger.info("Reproducing existing experiment '%s'.", rev[:7])
            raise exc
        logger.debug(
            "Stashed experiment '%s' for future execution.", stash_rev[:7]
        )
        return stash_rev

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
        for rev, item in to_run.items():
            self._scm_checkout(item.rev)
            self.scm.repo.git.stash("apply", rev)
            packed_args, packed_kwargs = self._unpack_args()
            checkpoint_reset = packed_kwargs.pop("checkpoint_reset", False)
            executor = LocalExecutor(
                rev=item.rev,
                baseline_rev=item.baseline_rev,
                branch=item.branch,
                repro_args=packed_args,
                repro_kwargs=packed_kwargs,
                dvc_dir=self.dvc_dir,
                cache_dir=self.repo.cache.local.cache_dir,
                checkpoint_reset=checkpoint_reset,
            )
            self._collect_input(executor)
            executors[rev] = executor

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
            self.scm.repo.git.stash("drop", index)

        result = {}
        for _, exp_result in exec_results.items():
            result.update(exp_result)
        return result

    def _reproduce(self, executors: dict, jobs: Optional[int] = 1) -> dict:
        """Run dvc repro for the specified ExperimentExecutors in parallel.

        Returns dict containing successfully executed experiments.
        """
        result = {}

        collect_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=jobs) as workers:
            futures = {}
            for rev, executor in executors.items():
                checkpoint_func = partial(
                    self._checkpoint_callback,
                    result,
                    collect_lock,
                    rev,
                    executor,
                )
                future = workers.submit(
                    executor.reproduce,
                    executor.dvc_dir,
                    cwd=executor.dvc.root_dir,
                    checkpoint_func=checkpoint_func,
                    **executor.repro_kwargs,
                )
                futures[future] = (rev, executor)

            for future in as_completed(futures):
                rev, executor = futures[future]
                exc = future.exception()

                try:
                    if exc is None:
                        stages = future.result()
                        self._collect_executor(
                            rev, executor, stages, result, collect_lock
                        )
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

    def _collect_executor(self, rev, executor, stages, result, lock):
        exp_hash = hash_exp(stages)
        checkpoint = any(stage.is_checkpoint for stage in stages)

        lock.acquire()
        try:
            # NOTE: GitPython Repo instances cannot be re-used
            # after process has received SIGINT or SIGTERM, so we
            # need this hack to re-instantiate git instances after
            # checkpoint runs. See:
            # https://github.com/gitpython-developers/GitPython/issues/427
            del self.repo.scm
            del self.scm

            if executor.branch:
                self._scm_checkout(executor.branch)
            else:
                self._scm_checkout(executor.rev)
            exp_rev = self._collect_and_commit(
                rev, executor, exp_hash, checkpoint=checkpoint
            )
            if exp_rev:
                logger.info("Reproduced experiment '%s'.", exp_rev[:7])
                result[rev] = {exp_rev: exp_hash}
        finally:
            lock.release()

    def _checkpoint_callback(
        self,
        result: Mapping,
        lock: threading.Lock,
        rev: str,
        executor: LocalExecutor,
        unchanged: Iterable,
        stages: Iterable,
    ):
        lock.acquire()
        try:
            if executor.branch:
                self._scm_checkout(executor.branch)
            else:
                self._scm_checkout(executor.rev)

            exp_hash = hash_exp(stages + unchanged)
            exp_rev = self._collect_and_commit(
                rev, executor, exp_hash, checkpoint=True
            )
            if exp_rev:
                if not executor.branch:
                    branch = self._get_branch_containing(exp_rev)
                    executor.branch = branch
                logger.info(
                    "Checkpoint experiment iteration '%s'.", exp_rev[:7]
                )
                result[rev] = {exp_rev: exp_hash}
        finally:
            lock.release()

    def _collect_and_commit(self, rev, executor, exp_hash, **kwargs):
        try:
            self._collect_output(executor)
        except DownloadError:
            logger.error(
                "Failed to collect output for experiment '%s'", rev,
            )
            return None
        finally:
            if os.path.exists(self.args_file):
                remove(self.args_file)

        try:
            create_branch = not executor.branch
            exp_rev = self._commit(
                exp_hash,
                baseline_rev=executor.baseline_rev,
                create_branch=create_branch,
                checkpoint_reset=executor.checkpoint_reset,
                **kwargs,
            )
        except UnchangedExperimentError as exc:
            logger.debug(
                "Experiment '%s' identical to '%s'", rev, exc.rev,
            )
            exp_rev = exc.rev
        return exp_rev

    def _collect_input(self, executor: ExperimentExecutor):
        """Copy (upload) input from the experiments workspace to the executor
        tree.
        """
        logger.debug("Collecting input for '%s'", executor.tmp_dir)
        repo_tree = RepoTree(self.exp_dvc)
        self._process(
            executor.tree,
            self.exp_dvc.tree,
            executor.collect_files(self.exp_dvc.tree, repo_tree),
        )

    def _collect_output(self, executor: ExperimentExecutor):
        """Copy (download) output from the executor tree into experiments
        workspace.
        """
        logger.debug("Collecting output from '%s'", executor.tmp_dir)
        self._process(
            self.exp_dvc.tree,
            executor.tree,
            executor.collect_output(),
            download=True,
        )

    @staticmethod
    def _process(dest_tree, src_tree, collected_files, download=False):
        from dvc.remote.base import _log_exceptions

        from_infos = []
        to_infos = []
        names = []
        for from_info in collected_files:
            from_infos.append(from_info)
            fname = from_info.relative_to(src_tree.path_info)
            names.append(str(fname))
            to_infos.append(dest_tree.path_info / fname)
        total = len(from_infos)

        if download:
            func = partial(
                _log_exceptions(src_tree.download, "download"),
                dir_mode=dest_tree.dir_mode,
            )
            desc = "Downloading"
        else:
            func = partial(_log_exceptions(dest_tree.upload, "upload"))
            desc = "Uploading"

        with Tqdm(total=total, unit="file", desc=desc) as pbar:
            func = pbar.wrap_fn(func)
            # TODO: parallelize this, currently --jobs for repro applies to
            # number of repro executors not download threads
            with ThreadPoolExecutor(max_workers=1) as dl_executor:
                mode = None
                stat_func = getattr(src_tree, "stat", None)
                futures = []
                for from_info, to_info, name in zip(
                    from_infos, to_infos, names
                ):
                    if stat_func:
                        mode = stat.S_IMODE(stat_func(from_info).st_mode)
                    futures.append(
                        dl_executor.submit(
                            func, from_info, to_info, name, file_mode=mode
                        )
                    )

                fails = sum(
                    future.result() for future in as_completed(futures)
                )

        if fails:
            if download:
                raise DownloadError(fails)
            raise UploadError(fails)

    @scm_locked
    def checkout_exp(self, rev, **kwargs):
        """Checkout an experiment to the user's workspace."""
        from git.exc import GitCommandError

        from dvc.repo.checkout import checkout as dvc_checkout

        baseline_rev = self._check_baseline(rev)
        self._scm_checkout(rev)

        branch = self._get_branch_containing(rev)
        m = self.BRANCH_RE.match(branch)
        if m and m.group("checkpoint"):
            kwargs.update({"allow_missing": True, "quiet": True})

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        self.scm.repo.head.commit.diff(
            baseline_rev, patch=True, full_index=True, binary=True, output=tmp
        )

        dirty = self.repo.scm.is_dirty(untracked_files=True)
        if dirty:
            logger.debug("Stashing workspace changes.")
            self.repo.scm.repo.git.stash("push", "--include-untracked")

        try:
            if os.path.getsize(tmp):
                logger.debug("Patching local workspace")
                self.repo.scm.repo.git.apply(tmp, reverse=True)
                need_checkout = True
            else:
                need_checkout = False
        except GitCommandError:
            raise DvcException("failed to apply experiment changes.")
        finally:
            remove(tmp)
            if dirty:
                self._unstash_workspace()

        if need_checkout:
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

    def _unstash_workspace(self):
        # Essentially we want `git stash pop` with `-X ours` merge strategy
        # to prefer the applied experiment changes over stashed workspace
        # changes. git stash doesn't support merge strategy parameters, but we
        # can do it ourselves with checkout/reset.
        from git.exc import GitCommandError

        logger.debug("Unstashing workspace changes.")
        git_repo = self.repo.scm.repo.git

        # stage workspace changes, then apply stashed changes on top
        git_repo.add(A=True)
        try:
            git_repo.stash("apply", "stash@{0}")
        except GitCommandError:
            # stash apply will return error code on merge conflicts,
            # prefer workspace changes over stash changes
            git_repo.checkout("--ours", "--", ".")

        # unstage changes and drop the stash entry
        git_repo.reset("HEAD")
        git_repo.stash("drop", "stash@{0}")

    @scm_locked
    def get_baseline(self, rev):
        """Return the baseline rev for an experiment rev."""
        return self._get_baseline(rev)

    def _get_baseline(self, rev):
        from git.exc import GitCommandError

        rev = self.scm.resolve_rev(rev)
        try:
            name = self.scm.repo.git.name_rev(rev, name_only=True)
        except GitCommandError:
            return None
        if name in ("undefined", "stash"):
            entry = self.stash_revs.get(rev)
            if entry:
                return entry.baseline_rev
            return None
        m = self.BRANCH_RE.match(name)
        if m:
            return self.scm.resolve_rev(m.group("baseline_rev"))
        return None

    def _get_branch_containing(self, rev):
        from git.exc import GitCommandError

        try:
            names = self.scm.repo.git.branch(contains=rev).strip().splitlines()

            if (
                names
                and self.scm.repo.head.is_detached
                and names[0].startswith("* (HEAD detached")
            ):
                # Ignore detached head entry if it exists
                del names[0]

            if not names:
                return None

            if len(names) > 1:
                raise MultipleBranchError(rev)
            name = names[0]
            if name.startswith("*"):
                name = name[1:]
            return name.rsplit("/")[-1].strip()
        except GitCommandError:
            pass
        return None

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
