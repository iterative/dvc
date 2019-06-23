from __future__ import unicode_literals

import os
import shutil
import logging
import shortuuid

from funcy import cached_property
from schema import Optional

from dvc.config import Config
from dvc.cache import CacheConfig
from dvc.exceptions import DvcException
from dvc.utils.compat import makedirs, str
from dvc.utils import remove


logger = logging.getLogger(__name__)


class ExternalRepoError(DvcException):
    pass


class NotInstalledError(ExternalRepoError):
    def __init__(self, name):
        super(NotInstalledError, self).__init__(
            "Repo '{}' is not installed".format(name)
        )


class InstallError(ExternalRepoError):
    def __init__(self, url, path, cause):
        super(InstallError, self).__init__(
            "Failed to install repo '{}' to '{}'".format(url, path),
            cause=cause,
        )


class RevError(ExternalRepoError):
    def __init__(self, url, rev, cause):
        super(RevError, self).__init__(
            "Failed to access revision '{}' for repo '{}'".format(rev, url),
            cause=cause,
        )


class ExternalRepo(object):
    REPOS_DIR = "repos"

    PARAM_URL = "url"
    PARAM_VERSION = "rev"

    SCHEMA = {Optional(PARAM_URL): str, Optional(PARAM_VERSION): str}

    def __init__(self, dvc_dir, **kwargs):
        self.repos_dir = os.path.join(dvc_dir, self.REPOS_DIR)
        self.url = kwargs[self.PARAM_URL]

        self.name = "{}-{}".format(os.path.basename(self.url), hash(self.url))

        self.rev = kwargs.get(self.PARAM_VERSION)
        self.path = os.path.join(self.repos_dir, self.name)

    @cached_property
    def repo(self):
        from dvc.repo import Repo

        if not self.installed:
            raise NotInstalledError(self.name)

        return Repo(self.path, rev=self.rev)

    @property
    def installed(self):
        return os.path.exists(self.path)

    def _install_to(self, tmp_dir, cache_dir):
        import git

        try:
            git.Repo.clone_from(
                self.url, tmp_dir, depth=1, no_single_branch=True
            )
        except git.exc.GitCommandError as exc:
            raise InstallError(self.url, tmp_dir, exc)

        if self.rev:
            try:
                repo = git.Repo(tmp_dir)
                repo.git.checkout(self.rev)
                repo.close()
            except git.exc.GitCommandError as exc:
                raise RevError(self.url, self.rev, exc)

        if cache_dir:
            from dvc.repo import Repo

            repo = Repo(tmp_dir)
            cache_config = CacheConfig(repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
            repo.scm.git.close()

    def install(self, cache_dir=None, force=False):
        if self.installed and not force:
            logger.info(
                "Skipping installing '{}'('{}') as it is already "
                "installed.".format(self.name, self.url)
            )
            return

        makedirs(self.repos_dir, exist_ok=True)

        # installing package to a temporary directory until we are sure that
        # it has been installed correctly.
        #
        # Note that we can't use tempfile.TemporaryDirectory is using symlinks
        # to tmpfs, so we won't be able to use move properly.
        tmp_dir = os.path.join(self.repos_dir, "." + str(shortuuid.uuid()))
        try:
            self._install_to(tmp_dir, cache_dir)
        except ExternalRepoError:
            if os.path.exists(tmp_dir):
                remove(tmp_dir)
            raise

        if self.installed:
            self.uninstall()

        shutil.move(tmp_dir, self.path)

    def uninstall(self):
        if not self.installed:
            logger.info(
                "Skipping uninstalling '{}' as it is not installed.".format(
                    self.name
                )
            )
            return

        remove(self.path)

    def update(self):
        self.repo.scm.fetch(self.rev)

    def dumpd(self):
        ret = {self.PARAM_URL: self.url}

        if self.rev:
            ret[self.PARAM_VERSION] = self.rev

        return ret
