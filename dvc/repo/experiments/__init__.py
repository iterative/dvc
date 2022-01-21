import logging
import os
import re
from functools import wraps
from typing import Dict, Iterable, List, Mapping, Optional, Type

from funcy import cached_property, first

from dvc.dependency.param import MissingParamsError
from dvc.env import DVCLIVE_RESUME
from dvc.exceptions import DvcException
from dvc.utils import relpath

from .base import (
    EXEC_APPLY,
    EXEC_BRANCH,
    EXEC_CHECKPOINT,
    EXEC_NAMESPACE,
    EXPS_NAMESPACE,
    EXPS_STASH,
    BaselineMismatchError,
    ExperimentExistsError,
    ExpRefInfo,
    ExpStashEntry,
    InvalidExpRefError,
    MultipleBranchError,
)
from .executor.base import (
    EXEC_PID_DIR,
    EXEC_TMP_DIR,
    BaseExecutor,
    ExecutorInfo,
)
from .executor.manager.base import BaseExecutorManager
from .executor.manager.local import (
    TempDirExecutorManager,
    WorkspaceExecutorManager,
)
from .executor.manager.ssh import SSHExecutorManager
from .utils import exp_refs_by_rev

logger = logging.getLogger(__name__)


def scm_locked(f):
    # Lock the experiments workspace so that we don't try to perform two
    # different sequences of git operations at once
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        from dvc.scm import map_scm_exception

        with map_scm_exception(), exp.scm_lock:
            return f(exp, *args, **kwargs)

    return wrapper


def unlocked_repo(f):
    @wraps(f)
    def wrapper(exp, *args, **kwargs):
        exp.repo.lock.unlock()
        exp.repo._reset()  # pylint: disable=protected-access
        try:
            ret = f(exp, *args, **kwargs)
        finally:
            exp.repo.lock.lock()
        return ret

    return wrapper


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    STASH_EXPERIMENT_FORMAT = "dvc-exp:{rev}:{baseline_rev}:{name}"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:commit: )"
        r"dvc-exp:(?P<rev>[0-9a-f]+):(?P<baseline_rev>[0-9a-f]+)"
        r":(?P<name>[^~^:\\?\[\]*]*)"
        r"(:(?P<branch>.+))?$"
    )
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
    def stash(self):
        from scmrepo.git import Stash

        return Stash(self.scm, EXPS_STASH)

    @property
    def stash_revs(self) -> Dict[str, ExpStashEntry]:
        revs = {}
        for i, entry in enumerate(self.stash):
            msg = entry.message.decode("utf-8").strip()
            m = self.STASH_EXPERIMENT_RE.match(msg)
            if m:
                revs[entry.new_sha.decode("utf-8")] = ExpStashEntry(
                    i,
                    m.group("rev"),
                    m.group("baseline_rev"),
                    m.group("branch"),
                    m.group("name"),
                )
        return revs

    def _stash_exp(
        self,
        *args,
        params: Optional[dict] = None,
        resume_rev: Optional[str] = None,
        baseline_rev: Optional[str] = None,
        branch: Optional[str] = None,
        name: Optional[str] = None,
        **kwargs,
    ):
        """Stash changes from the workspace as an experiment.

        Args:
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
                        if self.scm.is_dirty():
                            logger.info(
                                "Modified checkpoint experiment based on "
                                "'%s' will be created",
                                branch_name,
                            )
                            branch = None
                        elif (
                            not branch
                            or self.scm.get_ref(branch) != resume_rev
                        ):
                            msg = [
                                (
                                    "Nothing to do for unchanged checkpoint "
                                    f"'{resume_rev[:7]}'. "
                                )
                            ]
                            if branch:
                                msg.append(
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
                                msg.append(
                                    "To resume an experiment containing this "
                                    "checkpoint, apply one of these heads:\n"
                                    "\t{}".format(", ".join(names))
                                )
                            raise DvcException("".join(msg))
                        else:
                            logger.info(
                                "Existing checkpoint experiment '%s' will be "
                                "resumed",
                                branch_name,
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

        return stash_rev

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
    ):
        if not baseline_rev:
            baseline_rev = rev
        msg = self.STASH_EXPERIMENT_FORMAT.format(
            rev=rev, baseline_rev=baseline_rev, name=name if name else ""
        )
        if branch:
            return f"{msg}:{branch}"
        return msg

    def _pack_args(self, *args, **kwargs):
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

    def reproduce_one(
        self,
        queue: bool = False,
        tmp_dir: bool = False,
        checkpoint_resume: Optional[str] = None,
        reset: bool = False,
        machine: Optional[str] = None,
        **kwargs,
    ):
        """Reproduce and checkout a single experiment."""
        if queue and not checkpoint_resume:
            reset = True

        if reset:
            self.reset_checkpoints()

        if not (queue or tmp_dir or machine):
            staged, _, _ = self.scm.status()
            if staged:
                logger.warning(
                    "Your workspace contains staged Git changes which will be "
                    "unstaged before running this experiment."
                )
                self.scm.reset()

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

        stash_rev = self.new(
            checkpoint_resume=checkpoint_resume, reset=reset, **kwargs
        )
        if queue:
            logger.info(
                "Queued experiment '%s' for future execution.", stash_rev[:7]
            )
            return [stash_rev]
        if tmp_dir or queue:
            manager_cls: Type = TempDirExecutorManager
        elif machine:
            manager_cls = SSHExecutorManager
        else:
            manager_cls = WorkspaceExecutorManager
        results = self._reproduce_revs(
            revs=[stash_rev],
            keep_stash=False,
            manager_cls=manager_cls,
            machine=machine,
        )
        exp_rev = first(results)
        if exp_rev is not None:
            self._log_reproduced(results, tmp_dir=tmp_dir)
        return results

    def _workspace_resume_rev(self) -> Optional[str]:
        last_checkpoint = self._get_last_checkpoint()
        last_applied = self._get_last_applied()
        if last_checkpoint and last_applied:
            return last_applied
        return None

    def reproduce_queued(self, **kwargs):
        results = self._reproduce_revs(**kwargs)
        if results:
            self._log_reproduced(results, tmp_dir=True)
        return results

    def _log_reproduced(self, revs: Iterable[str], tmp_dir: bool = False):
        names = []
        for rev in revs:
            name = self.get_exact_name(rev)
            names.append(name if name else rev[:7])
        logger.info("\nReproduced experiment(s): %s", ", ".join(names))
        if tmp_dir:
            logger.info(
                "To apply the results of an experiment to your workspace "
                "run:\n\n"
                "\tdvc exp apply <exp>"
            )
        else:
            logger.info(
                "Experiment results have been applied to your workspace."
            )
        logger.info(
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
    def new(self, *args, checkpoint_resume: Optional[str] = None, **kwargs):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        if checkpoint_resume is not None:
            return self._resume_checkpoint(
                *args, resume_rev=checkpoint_resume, **kwargs
            )

        name = kwargs.get("name", None)
        baseline_sha = kwargs.get("baseline_rev") or self.repo.scm.get_rev()
        exp_ref = ExpRefInfo(baseline_sha=baseline_sha, name=name)

        try:
            self._validate_new_ref(exp_ref)
        except ExperimentExistsError as err:
            if not (kwargs.get("force", False) or kwargs.get("reset", False)):
                raise err

        return self._stash_exp(*args, **kwargs)

    def _resume_checkpoint(
        self, *args, resume_rev: Optional[str] = None, **kwargs
    ):
        """Resume an existing (checkpoint) experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
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
        return self._stash_exp(
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

    @scm_locked
    def _reproduce_revs(
        self,
        revs: Optional[Iterable] = None,
        keep_stash: Optional[bool] = True,
        manager_cls: Type = TempDirExecutorManager,
        machine: Optional[str] = None,
        **kwargs,
    ) -> Mapping[str, str]:
        """Reproduce the specified experiments.

        Args:
            revs: If revs is not specified, all stashed experiments will be
                reproduced.
            keep_stash: If True, stashed experiments will be preserved if they
                fail to reproduce successfully.

        Returns:
            dict mapping successfully reproduced experiment revs to their
            hashes.
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
                else ExpStashEntry(None, rev, rev, None, None)
                for rev in revs
            }

        logger.debug(
            "Reproducing experiment revs '%s'",
            ", ".join(rev[:7] for rev in to_run),
        )

        manager = manager_cls.from_stash_entries(
            self.scm,
            os.path.join(self.repo.tmp_dir, EXEC_TMP_DIR),
            self.repo,
            to_run,
            machine_name=machine,
        )
        try:
            exec_results = {}
            exec_results.update(self._executors_repro(manager, **kwargs))
        finally:
            # only drop successfully run stashed experiments
            to_drop: List[int] = [
                entry.stash_index
                for rev, entry in to_run.items()
                if (
                    entry.stash_index is not None
                    and (not keep_stash or rev in exec_results)
                )
            ]
            for index in sorted(to_drop, reverse=True):
                self.stash.drop(index)

        result: Dict[str, str] = {}
        for _, exp_result in exec_results.items():
            result.update(exp_result)
        return result

    @unlocked_repo
    def _executors_repro(
        self,
        manager: "BaseExecutorManager",
        **kwargs,
    ) -> Dict[str, Dict[str, str]]:
        """Run dvc repro for the specified BaseExecutors in parallel.

        Returns:
            dict mapping stash revs to the successfully executed experiments
            for each stash rev.
        """
        return manager.exec_queue(self.repo, **kwargs)

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

    @scm_locked
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

    def get_exact_name(self, rev: str):
        """Returns preferred name for the specified revision.

        Prefers tags, branches (heads), experiments in that orer.
        """
        exclude = f"{EXEC_NAMESPACE}/*"
        ref = self.scm.describe(rev, base=EXPS_NAMESPACE, exclude=exclude)
        if ref:
            try:
                name = ExpRefInfo.from_ref(ref).name
                if name:
                    return name
            except InvalidExpRefError:
                pass
        if rev in self.stash_revs:
            return self.stash_revs[rev].name
        return None

    def get_running_exps(self) -> Dict[str, int]:
        """Return info for running experiments."""
        from dvc.scm import InvalidRemoteSCMRepo
        from dvc.utils.serialize import load_json

        from .executor.local import TempDirExecutor

        result = {}
        pid_dir = os.path.join(
            self.repo.tmp_dir,
            EXEC_TMP_DIR,
            EXEC_PID_DIR,
        )
        for fname in self.repo.fs.find(pid_dir):
            rev, ext = os.path.splitext(os.path.basename(fname))
            if ext != BaseExecutor.INFOFILE_EXT:
                continue

            try:
                info = ExecutorInfo.from_dict(load_json(fname))
                if info.result is not None:
                    continue
                if rev == "workspace":
                    # If we are appending to a checkpoint branch in a workspace
                    # run, show the latest checkpoint as running.
                    last_rev = self.scm.get_ref(EXEC_BRANCH)
                    if last_rev:
                        result[last_rev] = info.asdict()
                    else:
                        result[rev] = info.asdict()
                else:
                    result[rev] = info.asdict()
                    if info.git_url:

                        def on_diverged(_ref: str, _checkpoint: bool):
                            return False

                        executor = TempDirExecutor.from_info(info)
                        try:
                            for ref in executor.fetch_exps(
                                self.scm,
                                on_diverged=on_diverged,
                            ):
                                logger.debug(
                                    "Updated running experiment '%s'.", ref
                                )
                                last_rev = self.scm.get_ref(ref)
                                result[rev]["last"] = last_rev
                                if last_rev:
                                    result[last_rev] = info.asdict()
                        except InvalidRemoteSCMRepo:
                            # ignore stale info files
                            del result[rev]
            except OSError:
                pass
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
