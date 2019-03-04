from __future__ import unicode_literals

import os
import git
import shutil
import tempfile
from git.cmd import Git

import dvc.logger as logger
from dvc.exceptions import DvcException


class PackageManager(object):
    PACKAGE_FILE='package.yaml'

    @staticmethod
    def read_packages():
        return []

    @staticmethod
    def get_package(addr):
        for pkg_class in [GitPackage]:
            return pkg_class(addr)
        return None

    def __init__(self, addr):
        self._addr = addr


class Package(object):
    MODULES_DIR = 'dvc_mod'

    def install_or_update(self, repo):
        raise NotImplementedError('A method of abstract Package class was called')

    def is_in_root(self):
        return True


class GitPackage(Package):
    GIT_DIR_TO_REMOVE = '.git'
    GIT_FILES_TO_REMOVE = ['.gitignore', '.gitmodules']

    def __init__(self, addr):
        self._addr = addr

    def install_or_update(self, repo):
        if not self.is_in_root():
            raise DvcException('This command can be run only from a repository root')

        if not os.path.exists(self.MODULES_DIR):
            logger.debug('Creating modules dir {}'.format(self.MODULES_DIR))
            os.makedirs(self.MODULES_DIR)
            repo.scm.ignore(os.path.abspath(self.MODULES_DIR))

        module_name = Git.polish_url(self._addr).strip('/').split('/')[-1]
        if not module_name:
            raise DvcException('Package address error: unable to extract package name')

        with ClonedTempGitRepo(self._addr, module_name) as tmpRepo:
            module_dir = os.path.join(GitPackage.MODULES_DIR, module_name)

            if os.path.exists(module_dir):
                logger.info('Updating package {}'.format(module_name))
                shutil.rmtree(module_dir)
            else:
                logger.info('Adding package {}'.format(module_name))
            tmpRepo.persist_to(module_dir)
        pass


class ClonedTempGitRepo(object):
    PREFIX = 'DVC_mod_'

    def __init__(self, addr, module_name):
        self._addr = addr
        self._tmp_mod_prefix = self.PREFIX + module_name + '_'
        self._cloned_tmp_dir = None

    def __enter__(self):
        if self._cloned_tmp_dir:
            raise DvcException('Git repo cloning duplication')

        module_temp_dir = None
        try:
            module_temp_dir = tempfile.mktemp(prefix=self._tmp_mod_prefix,
                                              dir=GitPackage.MODULES_DIR)
            logger.debug('Cloning git repository {} to temp dir {}'.format(
                self._addr, module_temp_dir)
            )
            git.Repo.clone_from(self._addr, module_temp_dir, depth=1)

            logger.debug(
                'Removing git meta files from {}: {} dif and {}'.format(
                    module_temp_dir,
                    GitPackage.GIT_DIR_TO_REMOVE,
                    ', '.join(GitPackage.GIT_FILES_TO_REMOVE)
                )
            )
            shutil.rmtree(os.path.join(module_temp_dir, GitPackage.GIT_DIR_TO_REMOVE))
            for item in GitPackage.GIT_FILES_TO_REMOVE:
                fname = os.path.join(module_temp_dir, item)
                if os.path.exists(fname):
                    os.remove(fname)

            self._cloned_tmp_dir = module_temp_dir
        finally:
            if not self._cloned_tmp_dir:
                if module_temp_dir and os.path.exists(module_temp_dir):
                    shutil.rmtree(module_temp_dir, ignore_errors=True)
        return self

    def persist_to(self, dir):
        if not self._cloned_tmp_dir:
            raise DvcException('...')
        shutil.move(self._cloned_tmp_dir, dir)
        self._cloned_tmp_dir = None

    def __exit__(self, exc_type, exc_value, traceback):
        if self._cloned_tmp_dir:
            shutil.rmtree(self._cloned_tmp_dir)
            self._cloned_tmp_dir = None
        pass


def install_pkg(self, address):
    """
    Install package.

    The command can be run only from DVC project root.

    E.g.
          Having: DVC package in https://github.com/dmpetrov/tag_classifier

          $ dvc pkg install https://github.com/dmpetrov/tag_classifier

          Result: tag_classifier package in dvc_mod/ directory
    """

    addresses = [address] if address else PackageManager.read_packages()

    for addr in addresses:
        try:
            pkg = PackageManager.get_package(addr)
            pkg.install_or_update(self)
        except Exception as ex:
            logger.error('Unable to install package: '.format(ex))
            return 1

    return 0
