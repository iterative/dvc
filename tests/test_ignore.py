import os

from dvc.ignore import DvcIgnoreFromFile, DvcIgnore, DvcIgnoreFileHandler
from dvc.utils.compat import cast_bytes
from tests.basic_env import TestDvc


class TestDvcIgnore(TestDvc):
    def setUp(self):
        super(TestDvcIgnore, self).setUp()
        self.ignore_file_handler = DvcIgnoreFileHandler(self.dvc.tree)

    def _get_all_paths(self):

        paths = []
        ignore_file_handler = DvcIgnoreFileHandler(self.dvc.tree)
        for root, dirs, files in self.dvc.tree.walk(
            self.dvc.root_dir, ignore_file_handler=ignore_file_handler
        ):
            for dname in dirs:
                paths.append(os.path.join(root, dname))

            for fname in files:
                paths.append(os.path.join(root, fname))

        return paths

    def test_ignore_comments(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write(os.path.basename(self.DATA))
            fobj.write(" #this is comment")

        ignore = DvcIgnoreFromFile(ignore_file, self.ignore_file_handler)

        self.assertEqual(1, len(ignore.patterns))

    def test_ignore_in_child_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_child_dir_unicode(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "wb") as fobj:
            fobj.write(cast_bytes(self.UNICODE, "utf-8"))

        forbidden_path = os.path.join(self.dvc.root_dir, self.UNICODE)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_parent_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")

        os.chdir(self.DATA_DIR)

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)
