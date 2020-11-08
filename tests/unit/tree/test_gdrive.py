import pytest

from dvc.path_info import GDriveURLInfo
from dvc.tree import GDriveTree
from tests.dir_helpers import load_gdrive_credentials


def test_get_file_name(dvc):
    # accessing the tests folder
    success = load_gdrive_credentials(dvc.root_dir)
    if not success:
        pytest.skip("no gdrive-user-credentials.json available")

    tree = GDriveTree(dvc, {})
    path_info = GDriveURLInfo("gdrive://1nKf4XcsNCN3oLujqlFTJoK5Fvx9iKCZb")
    filename = tree.get_file_name(path_info)
    assert filename == "data.txt"
