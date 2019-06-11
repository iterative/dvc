import os
import shutil
import logging

from funcy import cached_property
from schema import Optional

from dvc.config import Config
from dvc.cache import CacheConfig
from dvc.exceptions import DvcException
from dvc.utils.compat import urlparse


logger = logging.getLogger(__name__)


class NotInstalledPkgError(DvcException):
    def __init__(self, name):
        super(NotInstalledPkgError, self).__init__(
            "Package '{}' is not installed".format(name)
        )


class Pkg(object):
    PARAM_NAME = "name"
    PARAM_URL = "url"
    PARAM_VERSION = "version"

    SCHEMA = {
        Optional(PARAM_NAME): str,
        Optional(PARAM_URL): str,
        Optional(PARAM_VERSION): str,
    }

    def __init__(self, pkg_dir, **kwargs):
        self.pkg_dir = pkg_dir
        self.url = kwargs.get(self.PARAM_URL)

        name = kwargs.get(self.PARAM_NAME)
        if name is None:
            name = os.path.basename(self.url)
        self.name = name

        self.version = kwargs.get(self.PARAM_VERSION)
        self.path = os.path.join(pkg_dir, self.name)

    @cached_property
    def repo(self):
        from dvc.repo import Repo

        if not self.installed:
            raise NotInstalledPkgError(self.name)

        return Repo(self.path, version=self.version)

    @property
    def installed(self):
        return os.path.exists(self.path)

    def install(self, cache_dir=None):
        import git

        if self.installed:
            logger.info(
                "Skipping installing '{}'('{}') as it is already "
                "installed.".format(self.name, self.url)
            )
            return

        git.Repo.clone_from(
            self.url, self.path, depth=1, no_single_branch=True
        )

        if self.version:
            self.repo.scm.checkout(self.version)

        if cache_dir:
            cache_config = CacheConfig(self.repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)

    def uninstall(self):
        if not self.installed:
            logger.info(
                "Skipping uninstalling '{}' as it is not installed.".format(
                    self.name
                )
            )
            return

        shutil.rmtree(self.path)

    def update(self):
        self.repo.scm.fetch(self.version)

    def dumpd(self):
        ret = {self.PARAM_NAME: self.name}

        if self.url:
            ret[self.PARAM_URL] = self.url

        if self.version:
            ret[self.PARAM_VERSION] = self.version

        return ret


class PkgManager(object):
    PKG_DIR = "pkg"

    def __init__(self, repo):
        self.repo = repo
        self.pkg_dir = os.path.join(repo.dvc_dir, self.PKG_DIR)
        self.cache_dir = repo.cache.local.cache_dir

    def install(self, url, **kwargs):
        pkg = Pkg(self.pkg_dir, url=url, **kwargs)
        pkg.install(cache_dir=self.cache_dir)

    def uninstall(self, name):
        pkg = Pkg(self.pkg_dir, name=name)
        pkg.uninstall()

    def get_pkg(self, **kwargs):
        return Pkg(self.pkg_dir, **kwargs)

    def imp(self, name, src, out=None, version=None):
        scheme = urlparse(name).scheme

        if os.path.exists(name) or scheme:
            pkg = Pkg(self.pkg_dir, url=name)
            pkg.install(cache_dir=self.cache_dir)
        else:
            pkg = Pkg(self.pkg_dir, name=name)

        info = {Pkg.PARAM_NAME: pkg.name}
        if version:
            info[Pkg.PARAM_VERSION] = version

        self.repo.imp(src, out, pkg=info)
