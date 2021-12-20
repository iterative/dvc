import os

import pytest
from git import Repo

from dvc.scm import SCM, Git, NoSCM, SCMError


def test_init_none(tmp_dir):
    assert isinstance(SCM(os.fspath(tmp_dir), no_scm=True), NoSCM)


def test_init_git(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    assert isinstance(SCM(os.fspath(tmp_dir)), Git)


def test_init_no_git(tmp_dir):
    with pytest.raises(SCMError):
        SCM(os.fspath(tmp_dir))


def test_init_sub_dir(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    subdir = tmp_dir / "dir"
    subdir.mkdir()

    scm = SCM(os.fspath(subdir))
    assert scm.root_dir == os.fspath(tmp_dir)
