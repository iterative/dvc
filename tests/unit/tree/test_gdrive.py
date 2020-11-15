import os

import pytest

from dvc.tree import GDriveTree
from dvc.tree.gdrive import GDriveURLInfo


@pytest.mark.parametrize(
    "path, files",
    [
        (
            "16onq6BZiiUFj083XloYVk7LDDpklDr7h",
            ["file.txt", "dir/data.txt", "dir/another.txt"],
        ),
        (
            "16onq6BZiiUFj083XloYVk7LDDpklDr7h/dir",
            ["dir/data.txt", "dir/another.txt"],
        ),
    ],
)
def test_walk_files(dvc, path, files):
    if not os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA):
        pytest.skip("no gdrive credentials data available")

    path_info = GDriveURLInfo("gdrive://" + path)
    url = path_info.replace(path="").url

    tree = GDriveTree(dvc, {"url": url})
    results = tree.walk_files(path_info)
    list_results = list(results)

    assert len(list_results) == len(files)
    for r in list_results:
        assert r.path in files
