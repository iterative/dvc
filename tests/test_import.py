import os
import tempfile
from copy import copy

from dvc.command.import_file import CmdImportFile, ImportFileError
from tests.basic_env import DirHierarchyEnvironment


class TestCmdRemove(DirHierarchyEnvironment):
    def setUp(self):
        DirHierarchyEnvironment.init_environment(self)

    def test_import_file_to_dir(self):
        with tempfile.NamedTemporaryFile() as temp:
            content = 'Some data'
            temp.write(content)
            temp.flush()

            dir = os.path.join('data', self.dir1)
            settings = copy(self.settings)
            settings.set_args([temp.name, dir])
            cmd = CmdImportFile(settings)
            cmd.run()

            dvc_name = os.path.join(dir, os.path.basename(temp.name))
            data_item = self.path_factory.existing_data_item(dvc_name)

            self.assertTrue(os.path.exists(data_item.data.relative))
            self.assertTrue(os.path.islink(data_item.data.relative))
            self.assertEqual(open(data_item.data.relative).read(), content)

            self.assertTrue(os.path.exists(data_item.cache.relative))
            self.assertEqual(open(data_item.cache.relative).read(), content)

            self.assertTrue(os.path.exists(data_item.state.relative))
        pass

    def test_import_file_to_file(self):
        with tempfile.NamedTemporaryFile() as temp:
            content = 'Some data'
            temp.write(content)
            temp.flush()

            target_file_name = 'targ.txt'
            target_file = os.path.join('data', self.dir1, target_file_name)
            settings = copy(self.settings)
            settings.set_args([temp.name, target_file])
            cmd = CmdImportFile(settings)
            cmd.run()

            self.assertTrue(os.path.exists(target_file))
            self.assertTrue(open(target_file).read(), content)
        pass

    def test_import_to_existing_dir(self):
        with tempfile.NamedTemporaryFile() as temp:
            content = 'Some data'
            temp.write(content)
            temp.flush()

            dir = os.path.join('data', self.dir11)
            settings = copy(self.settings)
            settings.set_args([temp.name, dir])
            cmd = CmdImportFile(settings)
            cmd.run()

            dvc_name = os.path.join(dir, os.path.basename(temp.name))

            self.assertTrue(os.path.exists(dvc_name))
            self.assertTrue(open(dvc_name).read(), content)
        pass

    def test_import_to_not_existing_dir(self):
        with tempfile.NamedTemporaryFile() as temp:
            content = 'Some data'
            temp.write(content)
            temp.flush()

            dir = os.path.join('data', self.dir1, 'not_existing_dir/')
            settings = copy(self.settings)
            settings.set_args([temp.name, dir])
            cmd = CmdImportFile(settings)

            with self.assertRaises(ImportFileError):
                cmd.run()

            dvc_name = os.path.join(dir, os.path.basename(temp.name))

            self.assertFalse(os.path.exists(dir))
            self.assertFalse(os.path.exists(dvc_name))
        pass

    def test_multiple_import(self):
        with tempfile.NamedTemporaryFile() as temp1, tempfile.NamedTemporaryFile() as temp2:
            content = 'Some data'

            temp1.write(content)
            temp1.flush()

            temp2.write(content)
            temp2.flush()

            dir = os.path.join('data', self.dir11)
            settings = copy(self.settings)
            settings.set_args([temp1.name, temp2.name, dir])
            cmd = CmdImportFile(settings)
            cmd.run()

            dvc_name1 = os.path.join(dir, os.path.basename(temp1.name))
            dvc_name2 = os.path.join(dir, os.path.basename(temp2.name))

            self.assertTrue(os.path.exists(dvc_name1))
            self.assertTrue(open(dvc_name1).read(), content)

            self.assertTrue(os.path.exists(dvc_name2))
            self.assertTrue(open(dvc_name2).read(), content)
        pass