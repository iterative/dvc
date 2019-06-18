import os
import uuid
import shutil
import logging

from funcy import cached_property
from schema import Optional

from dvc.config import Config
from dvc.cache import CacheConfig
from dvc.path_info import PathInfo
from dvc.exceptions import DvcException
from dvc.utils.compat import urlparse


logger = logging.getLogger(__name__)


class PkgError(DvcException):
    pass


class NotInstalledError(PkgError):
    def __init__(self, name):
        super(NotInstalledError, self).__init__(
            "Package '{}' is not installed".format(name)
        )


class InstallError(PkgError):
    def __init__(self, url, path, cause):
        super(InstallError, self).__init__(
            "Failed to install pkg '{}' to '{}'".format(url, path), cause=cause
        )


class VersionError(PkgError):
    def __init__(self, url, version, cause):
        super(VersionError, self).__init__(
            "Failed to access version '{}' for package '{}'".format(
                version, url
            ),
            cause=cause,
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
            raise NotInstalledError(self.name)

        return Repo(self.path, version=self.version)

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

        if self.version:
            try:
                repo = git.Repo(tmp_dir)
                repo.git.checkout(self.version)
            except git.exc.GitCommandError as exc:
                raise VersionError(self.url, self.version, exc)

        if cache_dir:
            from dvc.repo import Repo

            repo = Repo(tmp_dir)
            cache_config = CacheConfig(repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)

        if self.installed:
            self.uninstall()

    def install(self, cache_dir=None, force=False):
        if self.installed and not force:
            logger.info(
                "Skipping installing '{}'('{}') as it is already "
                "installed.".format(self.name, self.url)
            )
            return

        # installing package to a temporary directory until we are sure that
        # it has been installed correctly.
        #
        # Note that tempfile.TemporaryDirectory is using symlinks to tmpfs, so
        # we won't be able to use move properly.
        tmp_dir = os.path.join(self.pkg_dir, "." + str(uuid.uuid4()))
        try:
            self._install_to(tmp_dir, cache_dir)
        except PkgError:
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
            raise

        shutil.move(tmp_dir, self.path)

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
        if not os.path.exists(self.pkg_dir):
            os.makedirs(self.pkg_dir)

        self.cache_dir = repo.cache.local.cache_dir

    def install(self, url, force=False, **kwargs):
        pkg = Pkg(self.pkg_dir, url=url, **kwargs)
        pkg.install(cache_dir=self.cache_dir, force=force)

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

    @classmethod
    def get(cls, url, src, out=None, version=None):
        if not out:
            out = os.path.basename(src)

        # Creating a directory right beside the output to make sure that they
        # are on the same filesystem, so we could take the advantage of
        # reflink and/or hardlink. Not using tempfile.TemporaryDirectory
        # because it will create a symlink to tmpfs, which defeats the purpose
        # and won't work with reflink/hardlink.
        dpath = os.path.dirname(os.path.abspath(out))
        tmp_dir = os.path.join(dpath, "." + str(uuid.uuid4()))
        try:
            pkg = Pkg(tmp_dir, url=url, version=version)
            pkg.install()
            # Try any links possible to avoid data duplication.
            #
            # Not using symlink, because we need to remove cache after we are
            # done, and to make that work we would have to copy data over
            # anyway before removing the cache, so we might just copy it
            # right away.
            #
            # Also, we can't use theoretical "move" link type here, because
            # the same cache file might be used a few times in a directory.
            pkg.repo.config.set(
                Config.SECTION_CACHE,
                Config.SECTION_CACHE_TYPE,
                "reflink,hardlink,copy",
            )
            src = os.path.join(pkg.path, urlparse(src).path.lstrip("/"))
            output, = pkg.repo.find_outs_by_path(src)
            pkg.repo.fetch(output.stage.path)
            output.path_info = PathInfo(os.path.abspath(out))
            with output.repo.state:
                output.checkout()
        finally:
            shutil.rmtree(tmp_dir)
