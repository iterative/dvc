import os

from dvc.repo.pkg.install import Package
from tests.basic_env import TestDvcPkg


class TestPkgBasic(TestDvcPkg):
    NEW_PKG_DIR = "new_pkg"

    def setUp(self):
        super(TestPkgBasic, self).setUp()

        self.to_dir = os.path.join(self._root_dir, self.NEW_PKG_DIR)
        os.mkdir(self.to_dir)
        self.dvc.pkg.install(self.pkg_fixture._root_dir, self.to_dir)

    def test_installed_dirs_and_files(self):
        self.assertTrue(os.path.isdir(self.pkg_dir))

        mod_dir_fullpath = os.path.join(self._root_dir, Package.MODULES_DIR)
        self.assertTrue(os.path.isdir(mod_dir_fullpath))

        installed_pkg_dir = os.path.join(mod_dir_fullpath, self.GIT_PKG)
        for file in [self.FOO, self.BAR, self.CODE]:
            installed_file = os.path.join(installed_pkg_dir, file)
            self.assertTrue(os.path.isfile(installed_file))

    def test_target_dir_checkout(self):
        pkg_data_file = os.path.join(self.to_dir, self.DATA)
        self.assertTrue(os.path.exists(pkg_data_file))
        self.assertTrue(os.path.isfile(pkg_data_file))

        pkg_data_dir = os.path.join(self.to_dir, self.DATA_SUB_DIR)
        self.assertTrue(os.path.exists(pkg_data_dir))
        self.assertTrue(os.path.isdir(pkg_data_dir))
