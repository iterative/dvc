from __future__ import unicode_literals

import os
import shutil
from git.cmd import Git

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.stage import Stage
from dvc.temp_git_repo import TempGitRepo


class PackageManager(object):
    PACKAGE_FILE = 'package.yaml'

    @staticmethod
    def read_packages():
        return []

    @staticmethod
    def get_package(addr):
        for pkg_class in [GitPackage]:
            try:
                return pkg_class(addr)
            except Exception:
                pass
        return None

    def __init__(self, addr):
        self._addr = addr


class Package(object):
    MODULES_DIR = 'dvc_mod'

    def install_or_update(self, repo, target_dir, outs_filter, stage_file):
        raise NotImplementedError('A method of abstract Package class was called')

    def is_in_root(self):
        return True


class GitPackage(Package):
    DEF_DVC_FILE_PREFIX = 'mod_'

    def __init__(self, addr):
        self._addr = addr

    def install_or_update(self, parent_repo, target_dir, outs_filter, stage):
        if not self.is_in_root():
            raise DvcException('This command can be run only from a repository root')

        if not os.path.exists(self.MODULES_DIR):
            logger.debug('Creating modules dir {}'.format(self.MODULES_DIR))
            os.makedirs(self.MODULES_DIR)
            parent_repo.scm.ignore(os.path.abspath(self.MODULES_DIR))

        module_name = Git.polish_url(self._addr).strip('/').split('/')[-1]
        if not module_name:
            raise DvcException('Package address error: unable to extract package name')

        fetched_stage_files = set()
        with TempGitRepo(self._addr, module_name, Package.MODULES_DIR) as tmp_repo:
            dvc_file = self.get_dvc_file_name(stage, target_dir, module_name)
            outputs_to_copy = self._outputs_to_copy(outs_filter, tmp_repo, target_dir)

            fetched_stage_files = set(map(lambda o: o.stage.path, outputs_to_copy))
            tmp_repo.fetch(fetched_stage_files)

            self._create_stage_file(outputs_to_copy, parent_repo, dvc_file)

            module_dir = os.path.join(GitPackage.MODULES_DIR, module_name)

            if os.path.exists(module_dir):
                logger.info('Updating package {}'.format(module_name))
                shutil.rmtree(module_dir)
            else:
                logger.info('Adding package {}'.format(module_name))

            tmp_repo.persist_to(module_dir, parent_repo)

        fetched_stage_files = parent_repo.stages(from_directory=target_dir)
        for stage in fetched_stage_files:
            parent_repo.checkout(stage.path)

        pass

    @staticmethod
    def _create_stage_file(outputs_to_copy, repo, dvc_file):
        stage = Stage.create(
            repo=repo,
            fname=dvc_file,
            validate_state=False
        )

        stage.outs = outputs_to_copy
        stage.dump()
        return stage

    def get_dvc_file_name(self, stage_file, target_dir, module_name):
        if stage_file:
            dvc_file_path = stage_file
        else:
            dvc_file_name = self.DEF_DVC_FILE_PREFIX + module_name + '.dvc'
            dvc_file_path = os.path.join(target_dir, dvc_file_name)
        return dvc_file_path

    @staticmethod
    def _outputs_to_copy(outs_filter, tmp_repo, target_dir):
        if not outs_filter:
            result = tmp_repo.outs
        else:
            result = list(filter(lambda out: out.dvc_path in outs_filter, tmp_repo.outs))

        from dvc.repo import Repo
        target_repo = Repo(target_dir)

        for r in result:
            r.repo = target_repo

        return result


def install_pkg(self, address, target_dir, outs_filter, file):
    """
    Install package.

    The command can be run only from DVC project root.

    E.g.
          Having: DVC package in https://github.com/dmpetrov/tag_classifier

          $ dvc pkg install https://github.com/dmpetrov/tag_classifier

          Result: tag_classifier package in dvc_mod/ directory
    """

    addresses = [address] if address else PackageManager.read_packages()

    if not os.path.isdir(target_dir):
        logger.error(
            'Unable to install package: target directory \'{}\' does not exist'
                .format(target_dir))
        return 1

    if not os.path.realpath(target_dir).startswith(os.path.realpath('.')):
        logger.error('Unable to install package: target directory {} should be'
                     ' a subdirectory of the current dir'.format(target_dir))
        return 1

    for addr in addresses:
        pkg = PackageManager.get_package(addr)
        try:
            pkg.install_or_update(self, target_dir, outs_filter, file)
        except Exception as ex:
            logger.error('Unable to install package: '.format(ex))
            return 1

    return 0
