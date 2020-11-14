import os

import pytest

from dvc.exceptions import FileMissingError
from dvc.tree import GDriveTree
from dvc.tree.gdrive import GDriveURLInfo
from tests.basic_env import TestDvc


class TestGDriveTree(TestDvc):
    def setUp(self):
        super().setUp()
        self.skip = not os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA)

        self.paths_answers = [
            ("1nKf4XcsNCN3oLujqlFTJoK5Fvx9iKCZb", "data.txt"),
            ("16onq6BZiiUFj083XloYVk7LDDpklDr7h/file.txt", "file.txt"),
            ("16onq6BZiiUFj083XloYVk7LDDpklDr7h/dir/data.txt", "data.txt"),
            ("root/test_data/data.txt", "data.txt"),
        ]

        self.invalid_paths = [
            "============",
            "16onq6BZiiUFj083XloYVk7LDDpklDr7h/fake_dir",
        ]

        self.dir_paths = [
            "16onq6BZiiUFj083XloYVk7LDDpklDr7h",
            "16onq6BZiiUFj083XloYVk7LDDpklDr7h/dir",
        ]

    def get_tree(self, path_info):
        if self.skip:
            pytest.skip("no gdrive credentials data available")

        url = path_info.replace(path="").url
        return GDriveTree(self.dvc, {"url": url})

    def test_get_file_name(self):
        for path, answer in self.paths_answers:
            path_info = GDriveURLInfo("gdrive://" + path)
            tree = self.get_tree(path_info)
            assert tree.exists(path_info)
            filename = tree.get_file_name(path_info)
            assert filename == answer

    def test_get_file_name_non_existing(self):
        for invalid_path in self.invalid_paths:
            path = "gdrive://" + invalid_path
            path_info = GDriveURLInfo(path)

            tree = self.get_tree(path_info)
            assert not tree.exists(path_info)

            with pytest.raises(FileMissingError) as e:
                _ = tree.get_file_name(path_info)
            assert path in str(e.value)

    def test_walk_files(self):
        for path in self.dir_paths:
            path_info = GDriveURLInfo("gdrive://" + path)
            tree = self.get_tree(path_info)
            files = tree.walk_files(path_info)
            list_files = list(files)
            assert list_files
