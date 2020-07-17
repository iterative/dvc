import logging
import os
import tempfile
from contextlib import contextmanager

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.scm.git import Git
from dvc.stage.serialize import to_lockfile
from dvc.utils import dict_sha256, env2bool, relpath
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


class UnchangedExperimentError(DvcException):
    pass


class Experiments:
    """Class that manages experiments in a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that these experiments belong to.
    """

    EXPERIMENTS_DIR = "experiments"

    def __init__(self, repo):
        if not (
            env2bool("DVC_TEST")
            or repo.config["core"].get("experiments", False)
        ):
            raise NotImplementedError

        self.repo = repo

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
    def exp_dvc_dir(self):
        dvc_dir = relpath(self.repo.dvc_dir, self.repo.scm.root_dir)
        return os.path.join(self.exp_dir, dvc_dir)

    @cached_property
    def exp_dvc(self):
        """Return clone dvc Repo instance."""
        from dvc.repo import Repo

        return Repo(self.exp_dvc_dir)

    @staticmethod
    def exp_hash(stages):
        exp_data = {}
        for stage in stages:
            exp_data.update(to_lockfile(stage))
        return dict_sha256(exp_data)

    @contextmanager
    def chdir(self):
        cwd = os.getcwd()
        os.chdir(self.exp_dvc.root_dir)
        yield
        os.chdir(cwd)

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
        if not Git.is_sha(rev) or not self.scm.has_rev(rev):
            self.scm.fetch(all=True)
        logger.debug("Checking out base experiment commit '%s'", rev)
        self.scm.checkout(rev)

    def _patch_exp(self):
        """Create a patch based on the current (parent) workspace and apply it
        to the experiment workspace.
        """
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        try:
            self.repo.scm.repo.git.diff(patch=True, output=tmp)
            if os.path.getsize(tmp):
                logger.debug("Patching experiment workspace")
                self.scm.repo.git.apply(tmp)
            else:
                raise UnchangedExperimentError(
                    "Experiment identical to baseline commit."
                )
        finally:
            remove(tmp)

    def _commit(self, stages, check_exists=True, branch=True, rev=None):
        """Commit stages as an experiment and return the commit SHA."""
        hash_ = self.exp_hash(stages)
        exp_name = f"{rev[:7]}-{hash_}"
        if branch:
            if check_exists and exp_name in self.scm.list_branches():
                logger.debug("Using existing experiment branch '%s'", exp_name)
                return self.scm.resolve_rev(exp_name)
            self.scm.checkout(exp_name, create_new=True)
        logger.debug("Commit new experiment branch '%s'", exp_name)
        self.scm.repo.git.add(A=True)
        self.scm.commit(f"Add experiment {exp_name}")
        return self.scm.get_rev()

    def _reproduce(self, *args, **kwargs):
        """Run `dvc repro` inside the experiments workspace."""
        with self.chdir():
            return self.exp_dvc.reproduce(*args, **kwargs)

    def new(self, *args, workspace=True, **kwargs):
        """Create a new experiment.

        Experiment will be reproduced and checked out into the user's
        workspace.
        """
        rev = self.repo.scm.get_rev()
        self._scm_checkout(rev)
        if workspace:
            try:
                self._patch_exp()
            except UnchangedExperimentError as exc:
                logger.info("Reproducing existing experiment '%s'.", rev[:7])
                raise exc
        else:
            # configure params via command line here
            pass
        self.exp_dvc.checkout()
        stages = self._reproduce(*args, **kwargs)
        exp_rev = self._commit(stages, rev=rev)
        self.checkout_exp(exp_rev, force=True)
        logger.info("Generated experiment '%s'.", exp_rev[:7])
        return stages

    def checkout_exp(self, rev, force=False):
        """Checkout an experiment to the user's workspace."""
        from git.exc import GitCommandError
        from dvc.repo.checkout import _checkout as dvc_checkout

        if force:
            self.repo.scm.repo.git.reset(hard=True)
        logger.debug(f"checkout {rev}")
        self._scm_checkout(rev)

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        self.scm.repo.head.commit.diff("HEAD~1", patch=True, output=tmp)
        try:
            if os.path.getsize(tmp):
                logger.debug("Patching local workspace")
                self.repo.scm.repo.git.apply(tmp, reverse=True)
            dvc_checkout(self.repo)
        except GitCommandError:
            raise DvcException(
                "Checkout failed, experiment contains changes which "
                "conflict with your current workspace. To overwrite "
                "your workspace, use `dvc experiments checkout --force`."
            )
        finally:
            remove(tmp)

    def checkout(self, *args, **kwargs):
        from dvc.repo.experiments.checkout import checkout

        return checkout(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from dvc.repo.experiments.diff import diff

        return diff(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)
