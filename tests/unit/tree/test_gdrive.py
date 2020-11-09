import pytest

from dvc.exceptions import DvcException
from dvc.tree import GDriveTree
from dvc.tree.gdrive import GDriveURLInfo
from tests.basic_env import TestDvc
from tests.utils import load_gdrive_credentials


class TestGDriveURL(TestDvc):
    def setUp(self):
        super().setUp()
        success = load_gdrive_credentials(self.dvc.root_dir)
        if not success:
            pytest.skip("no gdrive-user-credentials.json available")

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

        self.tree = GDriveTree(self.dvc, {})

    def test_get_file_name(self):
        for path, answer in self.paths_answers:
            path_info = GDriveURLInfo("gdrive://" + path)
            filename = self.tree.get_file_name(path_info)
            assert filename == answer

    def test_get_file_name_non_existing(self):
        for invalid_path in self.invalid_paths:
            path = "gdrive://" + invalid_path
            path_info = GDriveURLInfo(path)
            with pytest.raises(DvcException) as e:
                _ = self.tree.get_file_name(path_info)
            assert path in str(e.value)
            assert "doesn't exist" in str(e.value)
