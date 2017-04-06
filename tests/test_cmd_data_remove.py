import os

from dvc.command.remove import CmdDataRemove, DataRemoveError
from dvc.path.data_item import DataItemError
from tests.basic_env import DirHierarchyEnvironment


class TestCmdDataRemove(DirHierarchyEnvironment):
    def setUp(self):
        DirHierarchyEnvironment.init_environment(self)

    def test_file_by_file_removal(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        dir1_dvc_name = os.path.join('data', self.dir1)
        self.assertTrue(os.path.exists(dir1_dvc_name))

        cmd.remove_dir_file_by_file(dir1_dvc_name)
        self.assertFalse(os.path.exists(dir1_dvc_name))

    def test_remove_deep_dir(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True
        cmd.parsed_args.recursive = True

        dir1_dvc_name = os.path.join('data', self.dir1)
        self.assertTrue(os.path.exists(dir1_dvc_name))

        cmd.remove_dir(dir1_dvc_name)
        self.assertFalse(os.path.exists(dir1_dvc_name))

    def test_not_recursive_removal(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True
        cmd.parsed_args.recursive = False

        dir1_dvc_name = os.path.join('data', self.dir1)
        self.assertTrue(os.path.exists(dir1_dvc_name))

        with self.assertRaises(DataRemoveError):
            cmd.remove_dir(dir1_dvc_name)
        pass

    def test_data_dir_removal(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True
        cmd.parsed_args.recursive = True

        data_dir = 'data'
        self.assertTrue(os.path.exists(data_dir))

        with self.assertRaises(DataRemoveError):
            cmd.remove_dir(data_dir)
        pass


class TestRemoveDataItem(DirHierarchyEnvironment):
    def setUp(self):
        DirHierarchyEnvironment.init_environment(self)

    def test_remove_data_instance(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        self.assertTrue(os.path.isfile(self.file1))
        self.assertTrue(os.path.isfile(self.cache1))
        self.assertTrue(os.path.isfile(self.state1))

        cmd.remove_file(self.file1)
        self.assertFalse(os.path.exists(self.file1))
        self.assertFalse(os.path.exists(self.cache1))
        self.assertFalse(os.path.exists(self.state1))

    def test_keep_cache(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True
        cmd.parsed_args.keep_in_cache = True

        self.assertTrue(os.path.isfile(self.file1))
        self.assertTrue(os.path.isfile(self.cache1))
        self.assertTrue(os.path.isfile(self.state1))

        cmd.remove_file(self.file1)
        self.assertFalse(os.path.exists(self.file1))
        self.assertTrue(os.path.exists(self.cache1))
        self.assertFalse(os.path.exists(self.state1))

    def test_remove_data_instance_without_cache(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        self.assertTrue(os.path.isfile(self.file6))
        self.assertIsNone(self.cache6)
        self.assertTrue(os.path.isfile(self.state6))

        with self.assertRaises(DataItemError):
            cmd.remove_file(self.file6)


class TestRemoveEndToEnd(DirHierarchyEnvironment):
    def setUp(self):
        DirHierarchyEnvironment.init_environment(self)

    def test_end_to_end_with_an_error(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        dir11_full = os.path.join('data', self.dir11)
        dir2_full = os.path.join('data', self.dir2)
        file6_full = os.path.join('data', self.file6)

        cmd.parsed_args.target = [dir11_full, self.file5, file6_full]
        cmd.parsed_args.recursive = True

        cmd.parsed_args.skip_git_actions = True

        self.assertTrue(os.path.exists(dir11_full))
        self.assertTrue(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))
        self.assertTrue(os.path.exists(self.file6))

        self.assertFalse(cmd.remove_all_targets())

        self.assertFalse(os.path.exists(dir11_full))
        self.assertFalse(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))
        self.assertTrue(os.path.exists(self.file6))

    def test_end_to_end_with_no_error(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        dir11_full = os.path.join('data', self.dir11)
        dir2_full = os.path.join('data', self.dir2)

        cmd.parsed_args.target = [dir11_full, self.file5]
        cmd.parsed_args.recursive = True

        cmd.parsed_args.skip_git_actions = True

        self.assertTrue(os.path.exists(dir11_full))
        self.assertTrue(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))

        self.assertTrue(cmd.remove_all_targets())

        self.assertFalse(os.path.exists(dir11_full))
        self.assertFalse(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))

    def test_run(self):
        cmd = CmdDataRemove(self.settings)
        cmd.parsed_args.keep_in_cloud = True

        dir11_full = os.path.join('data', self.dir11)
        dir2_full = os.path.join('data', self.dir2)

        cmd.parsed_args.target = [dir11_full, self.file5]
        cmd.parsed_args.recursive = True

        cmd.parsed_args.skip_git_actions = True

        self.assertTrue(os.path.exists(dir11_full))
        self.assertTrue(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))

        self.assertEqual(cmd.run(), 0)

        self.assertFalse(os.path.exists(dir11_full))
        self.assertFalse(os.path.exists(self.file5))
        self.assertTrue(os.path.exists(dir2_full))
        self.assertTrue(os.path.exists(self.file1))
