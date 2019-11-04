from __future__ import unicode_literals

import filecmp
import os

import pytest

from dvc.repo import Repo
from dvc.utils import makedirs


def test_get_file(repo_dir):
    src = repo_dir.FOO
    dst = repo_dir.FOO + "_imported"

    Repo.get_url(src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_get_url_to_dir(dname, repo_dir):
    src = repo_dir.DATA

    makedirs(dname, exist_ok=True)

    Repo.get_url(src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert os.path.isdir(dname)
    assert filecmp.cmp(repo_dir.DATA, dst, shallow=False)
