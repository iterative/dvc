import logging
import os
import tempfile
from contextlib import contextmanager

from funcy import cached_property

from dvc.exceptions import DvcException
from dvc.scm.git import Git
from dvc.utils import relpath
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

    @contextmanager
    def _chdir(self):
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
            self.scm.pull()
        logger.debug("Checking out base experiment commit '%s'", rev)
        self.scm.checkout(rev)

    def _patch_exp(self):
        """Create a patch based on the current (parent) workspace and apply it
        to the experiment workspace.
        """
        logger.debug("Patching experiment workspace")
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        self.repo.scm.repo.git.diff(patch=True, output=tmp)
        self.scm.repo.git.apply(tmp)
        remove(tmp)

    def reproduce(self, *args, **kwargs):
        rev = self.repo.scm.get_rev()
        self._scm_checkout(rev)
        self._patch_exp()
        with self._chdir():
            self.exp_dvc.checkout()
            return self.exp_dvc.reproduce(*args, **kwargs)

    def diff(self, *args, **kwargs):
        pass

    def list(self, *args, **kwargs):
        pass

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)
