from __future__ import unicode_literals

import os
import shutil
import logging

from dvc.exceptions import DvcException
from dvc.stage import Stage
from dvc.scm.git.temp_git_repo import TempGitRepo


logger = logging.getLogger(__name__)


class PackageManager(object):
    PACKAGE_FILE = "package.yaml"

    @staticmethod
    def read_packages():
        return []

    @staticmethod
    def get_package(addr):
        for pkg_class in [GitPackage]:
            try:
                return pkg_class()
            except Exception:
                pass
        return None

    def __init__(self, addr):
        self._addr = addr


class Package(object):
    MODULES_DIR = "dvc_mod"

    def install_or_update(
        self, parent_repo, address, target_dir, select=[], fname=None
    ):
        raise NotImplementedError(
            "A method of abstract Package class was called"
        )

    def is_in_root(self):
        return True


class GitPackage(Package):
    DEF_DVC_FILE_PREFIX = "mod_"

    def install_or_update(
        self, parent_repo, address, target_dir, select=[], fname=None
    ):
        from git.cmd import Git

        if not self.is_in_root():
            raise DvcException(
                "This command can be run only from a repository root"
            )

        if not os.path.exists(self.MODULES_DIR):
            logger.debug("Creating modules dir {}".format(self.MODULES_DIR))
            os.makedirs(self.MODULES_DIR)
            parent_repo.scm.ignore(os.path.abspath(self.MODULES_DIR))

        module_name = Git.polish_url(address).strip("/").split("/")[-1]
        if not module_name:
            raise DvcException(
                "Package address error: unable to extract package name"
            )

        with TempGitRepo(
            address, module_name, Package.MODULES_DIR
        ) as tmp_repo:
            outputs_to_copy = tmp_repo.outs
            if select:
                outputs_to_copy = list(
                    filter(lambda out: out.dvc_path in select, outputs_to_copy)
                )

            fetched_stage_files = set(
                map(lambda o: o.stage.path, outputs_to_copy)
            )
            tmp_repo.fetch(fetched_stage_files)

            module_dir = self.create_module_dir(module_name)
            tmp_repo.persist_to(module_dir, parent_repo)

            dvc_file = self.get_dvc_file_name(fname, target_dir, module_name)
            try:
                self.persist_stage_and_scm_state(
                    parent_repo, outputs_to_copy, target_dir, dvc_file
                )
            except Exception as ex:
                raise DvcException(
                    "Package '{}' was installed "
                    "but stage file '{}' "
                    "was not created properly: {}".format(
                        address, dvc_file, ex
                    )
                )

        parent_repo.checkout(dvc_file)

    @staticmethod
    def persist_stage_and_scm_state(
        parent_repo, outputs_to_copy, target_dir, dvc_file
    ):
        stage = Stage.create(
            repo=parent_repo,
            fname=dvc_file,
            validate_state=False,
            wdir=target_dir,
        )
        stage.outs = list(
            map(lambda o: o.assign_to_stage_file(stage), outputs_to_copy)
        )

        for out in stage.outs:
            parent_repo.scm.ignore(out.path, in_curr_dir=True)

        stage.dump()

    @staticmethod
    def create_module_dir(module_name):
        module_dir = os.path.join(GitPackage.MODULES_DIR, module_name)
        if os.path.exists(module_dir):
            logger.info("Updating package {}".format(module_name))
            shutil.rmtree(module_dir)
        else:
            logger.info("Adding package {}".format(module_name))
        return module_dir

    def get_dvc_file_name(self, stage_file, target_dir, module_name):
        if stage_file:
            dvc_file_path = stage_file
        else:
            dvc_file_name = self.DEF_DVC_FILE_PREFIX + module_name + ".dvc"
            dvc_file_path = os.path.join(target_dir, dvc_file_name)
        return dvc_file_path


def install(self, address, target_dir, select=[], fname=None):
    """
    Install package.

    The command can be run only from DVC project root.

    E.g.
          Having: DVC package in https://github.com/dmpetrov/tag_classifier

          $ dvc pkg install https://github.com/dmpetrov/tag_classifier

          Result: tag_classifier package in dvc_mod/ directory
    """

    if not os.path.isdir(target_dir):
        raise DvcException(
            "target directory '{}' does not exist".format(target_dir)
        )

    curr_dir = os.path.realpath(os.curdir)
    if not os.path.realpath(target_dir).startswith(curr_dir):
        raise DvcException(
            "the current directory should be a subdirectory of the target "
            "dir '{}'".format(target_dir)
        )

    addresses = [address] if address else PackageManager.read_packages()
    for addr in addresses:
        mgr = PackageManager.get_package(addr)
        mgr.install_or_update(self, address, target_dir, select, fname)
