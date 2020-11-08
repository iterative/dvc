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

        self.tree = GDriveTree(self.dvc, {})

    def test_get_file_name(self):

        path_info = GDriveURLInfo("gdrive://1nKf4XcsNCN3oLujqlFTJoK5Fvx9iKCZb")
        filename = self.tree.get_file_name(path_info)
        assert filename == "data.txt"

    def test_get_file_name_non_existing(self):
        path = "gdrive://=================="
        path_info = GDriveURLInfo(path)
        with pytest.raises(DvcException) as e:
            _ = self.tree.get_file_name(path_info)
        assert path in str(e.value)
        assert "doesn't exist" in str(e.value)
