import logging
import os
import re
import tempfile
from collections import defaultdict
from collections.abc import Mapping
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    as_completed,
)
from contextlib import contextmanager
from functools import partial, wraps
from typing import Iterable, Optional

from funcy import cached_property, first

from dvc.exceptions import DownloadError, DvcException
from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.repo.experiments.executor import ExperimentExecutor, LocalExecutor
from dvc.scm.git import Git
from dvc.stage.serialize import to_lockfile
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
        exp_data.update(to_lockfile(stage))
    return dict_sha256(exp_data)


class UnchangedExperimentError(DvcException):
    def __init__(self, rev):
        super().__init__(f"Experiment identical to baseline '{rev[:7]}'.")
        self.rev = rev


class BaselineMismatchError(DvcException):
    def __init__(self, rev, expected):
        rev_str = f"{rev[:7]}" if rev is not None else "dangling commit"
        super().__init__(
            f"Experiment derived from '{rev_str}', expected '{expected[:7]}'."
        )
        self.rev = rev
        self.expected_rev = expected


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    EXPERIMENTS_DIR = "experiments"
    PACKED_ARGS_FILE = "repro.dat"
    STASH_MSG_PREFIX = "dvc-exp-"
    STASH_EXPERIMENT_RE = re.compile(
        r"(?:On \(.*\): )dvc-exp-(?P<baseline_rev>[0-9a-f]+)$"
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
        if "refs/stash" in self.scm.repo.refs:
            return self.scm.repo.refs["refs/stash"].log()
        return []

    @property
    def stash_revs(self):
        revs = {}
        for i, entry in enumerate(self.stash_reflog):
            m = self.STASH_EXPERIMENT_RE.match(entry.message)
            if m:
                revs[entry.newhexsha] = (i, m.group("baseline_rev"))
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
        if self.scm.repo.head.is_detached:
            # switch back to default branch
            self.scm.repo.heads[0].checkout()
        if not Git.is_sha(rev) or not self.scm.has_rev(rev):
            self.scm.pull()
        logger.debug("Checking out experiment commit '%s'", rev)
        self.scm.checkout(rev)

    def _stash_exp(self, *args, params: Optional[dict] = None, **kwargs):
        """Stash changes from the current (parent) workspace as an experiment.

        Args:
            params: Optional dictionary of parameter values to be used.
                Values take priority over any parameters specified in the
                user's workspace.
        """
        rev = self.scm.get_rev()

        # patch user's workspace into experiments clone
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        try:
            self.repo.scm.repo.git.diff(patch=True, output=tmp)
            if os.path.getsize(tmp):
                logger.debug("Patching experiment workspace")
                self.scm.repo.git.apply(tmp)
            elif not params:
                # experiment matches original baseline
                raise UnchangedExperimentError(rev)
        finally:
            remove(tmp)

        # update experiment params from command line
        if params:
            self._update_params(params)

        # save additional repro command line arguments
        self._pack_args(*args, **kwargs)

        # save experiment as a stash commit w/message containing baseline rev
        # (stash commits are merge commits and do not contain a parent commit
        # SHA)
        msg = f"{self.STASH_MSG_PREFIX}{rev}"
        self.scm.repo.git.stash("push", "-m", msg)
        return self.scm.resolve_rev("stash@{0}")

    def _pack_args(self, *args, **kwargs):
        ExperimentExecutor.pack_repro_args(self.args_file, *args, **kwargs)
        self.scm.add(self.args_file)

    def _unpack_args(self, tree=None):
        return ExperimentExecutor.unpack_repro_args(self.args_file, tree=tree)

    def _update_params(self, params: dict):
        """Update experiment params files with the specified values."""
        from dvc.utils.serialize import (
            dump_toml,
            dump_yaml,
            parse_toml_for_update,
            parse_yaml_for_update,
        )

        logger.debug("Using experiment params '%s'", params)

        # recursive dict update
        def _update(dict_, other):
            for key, value in other.items():
                if isinstance(value, Mapping):
                    dict_[key] = _update(dict_.get(key, {}), value)
                else:
                    dict_[key] = value
            return dict_

        loaders = defaultdict(lambda: parse_yaml_for_update)
        loaders.update({".toml": parse_toml_for_update})
        dumpers = defaultdict(lambda: dump_yaml)
        dumpers.update({".toml": dump_toml})

        for params_fname in params:
            path = PathInfo(self.exp_dvc.root_dir) / params_fname
            with self.exp_dvc.tree.open(path, "r") as fobj:
                text = fobj.read()
            suffix = path.suffix.lower()
            data = loaders[suffix](text, path)
            _update(data, params[params_fname])
            dumpers[suffix](path, data)

    def _commit(self, exp_hash, check_exists=True, branch=True):
        """Commit stages as an experiment and return the commit SHA."""
        if not self.scm.is_dirty():
            raise UnchangedExperimentError(self.scm.get_rev())
        rev = self.scm.get_rev()
        exp_name = f"{rev[:7]}-{exp_hash}"
        if branch:
            if check_exists and exp_name in self.scm.list_branches():
                logger.debug("Using existing experiment branch '%s'", exp_name)
                return self.scm.resolve_rev(exp_name)
            self.scm.checkout(exp_name, create_new=True)
        logger.debug("Commit new experiment branch '%s'", exp_name)
        self.scm.repo.git.add(A=True)
        self.scm.commit(f"Add experiment {exp_name}")
        return self.scm.get_rev()

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
    def new(self, *args, **kwargs):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        rev = self.repo.scm.get_rev()
        self._scm_checkout(rev)
        try:
            stash_rev = self._stash_exp(*args, **kwargs)
        except UnchangedExperimentError as exc:
            logger.info("Reproducing existing experiment '%s'.", rev[:7])
            raise exc
        logger.debug(
            "Stashed experiment '%s' for future execution.", stash_rev[:7]
        )
        return stash_rev

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
        #   input_rev: (stash_index, baseline_rev)
        # where input_rev contains the changes to execute (usually a stash
        # commit) and baseline_rev is the baseline to compare output against.
        # The final experiment commit will be branched from baseline_rev.
        if revs is None:
            to_run = {
                rev: baseline_rev
                for rev, (_, baseline_rev) in stash_revs.items()
            }
        else:
            to_run = {
                rev: stash_revs[rev][1] if rev in stash_revs else rev
                for rev in revs
            }

        # setup executors
        executors = {}
        for rev, baseline_rev in to_run.items():
            tree = self.scm.get_tree(rev)
            repro_args, repro_kwargs = self._unpack_args(tree)
            executor = LocalExecutor(
                tree,
                baseline_rev,
                repro_args=repro_args,
                repro_kwargs=repro_kwargs,
                dvc_dir=self.dvc_dir,
                cache_dir=self.repo.cache.local.cache_dir,
            )
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

        with ProcessPoolExecutor(max_workers=jobs) as workers:
            futures = {}
            for rev, executor in executors.items():
                future = workers.submit(
                    executor.reproduce,
                    executor.dvc_dir,
                    cwd=executor.dvc.root_dir,
                    **executor.repro_kwargs,
                )
                futures[future] = (rev, executor)
            for future in as_completed(futures):
                rev, executor = futures[future]
                exc = future.exception()
                if exc is None:
                    exp_hash = future.result()
                    self._scm_checkout(executor.baseline_rev)
                    try:
                        self._collect_output(executor)
                    except DownloadError:
                        logger.error(
                            "Failed to collect output for experiment '%s'",
                            rev,
                        )
                        continue
                    finally:
                        if os.path.exists(self.args_file):
                            remove(self.args_file)

                    try:
                        exp_rev = self._commit(exp_hash)
                    except UnchangedExperimentError:
                        logger.debug(
                            "Experiment '%s' identical to baseline '%s'",
                            rev,
                            executor.baseline_rev,
                        )
                        exp_rev = executor.baseline_rev
                    logger.info("Reproduced experiment '%s'.", exp_rev[:7])
                    result[rev] = {exp_rev: exp_hash}
                else:
                    logger.exception(
                        "Failed to reproduce experiment '%s'", rev
                    )
                executor.cleanup()

        return result

    def _collect_output(self, executor: ExperimentExecutor):
        """Copy (download) output from the executor tree into experiments
        workspace.
        """
        from dvc.cache.local import _log_exceptions

        logger.debug("Collecting output from '%s'", executor.tmp_dir)
        dest_tree = self.exp_dvc.tree
        src_tree = executor.tree

        from_infos = []
        to_infos = []
        names = []
        for from_info in executor.collect_output():
            from_infos.append(from_info)
            fname = from_info.relative_to(src_tree.path_info)
            names.append(str(fname))
            to_info = dest_tree.path_info / fname
            to_infos.append(dest_tree.path_info / fname)
            logger.debug(f"from '{from_info}' to '{to_info}'")
        total = len(from_infos)

        func = partial(
            _log_exceptions(dest_tree.download, "download"),
            dir_mode=dest_tree.dir_mode,
            file_mode=dest_tree.file_mode,
        )
        with Tqdm(total=total, unit="file", desc="Downloading") as pbar:
            func = pbar.wrap_fn(func)
            # TODO: parallelize this, currently --jobs for repro applies to
            # number of repro executors not download threads
            with ThreadPoolExecutor(max_workers=1) as dl_executor:
                fails = sum(dl_executor.map(func, from_infos, to_infos, names))

        if fails:
            raise DownloadError(fails)

    @scm_locked
    def checkout_exp(self, rev):
        """Checkout an experiment to the user's workspace."""
        from git.exc import GitCommandError

        from dvc.repo.checkout import _checkout as dvc_checkout

        self._check_baseline(rev)
        self._scm_checkout(rev)

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        self.scm.repo.head.commit.diff("HEAD~1", patch=True, output=tmp)

        dirty = self.repo.scm.is_dirty()
        if dirty:
            logger.debug("Stashing workspace changes.")
            self.repo.scm.repo.git.stash("push")

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
            dvc_checkout(self.repo)

    def _check_baseline(self, exp_rev):
        baseline_sha = self.repo.scm.get_rev()
        exp_commit = self.scm.repo.rev_parse(exp_rev)
        parent = first(exp_commit.parents)
        if parent is not None and parent.hexsha == baseline_sha:
            return
        raise BaselineMismatchError(parent, baseline_sha)

    def _unstash_workspace(self):
        # Essentially we want `git stash pop` with `-X ours` merge strategy
        # to prefer the applied experiment changes over stashed workspace
        # changes. git stash doesn't support merge strategy parameters, but we
        # can do it ourselves with checkout/reset.
        logger.debug("Unstashing workspace changes.")
        self.repo.scm.repo.git.checkout("--ours", "stash@{0}", "--", ".")
        self.repo.scm.repo.git.reset("HEAD")
        self.repo.scm.repo.git.stash("drop", "stash@{0}")

    def checkout(self, *args, **kwargs):
        from dvc.repo.experiments.checkout import checkout

        return checkout(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from dvc.repo.experiments.diff import diff

        return diff(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)
