import os

import pytest
from git import Repo

from dvc.scm import SCM, Git, NoSCM, SCMError, lfs_prefetch


def test_init_none(tmp_dir):
    assert isinstance(SCM(os.fspath(tmp_dir), no_scm=True), NoSCM)


def test_init_git(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    assert isinstance(SCM(os.fspath(tmp_dir)), Git)


def test_init_no_git(tmp_dir):
    with pytest.raises(SCMError, match=r".* is not a git repository"):
        SCM(os.fspath(tmp_dir))


def test_init_sub_dir(tmp_dir):
    Repo.init(os.fspath(tmp_dir))
    subdir = tmp_dir / "dir"
    subdir.mkdir()

    scm = SCM(os.fspath(subdir))
    assert scm.root_dir == os.fspath(tmp_dir)


def test_lfs_prefetch(tmp_dir, dvc, scm, mocker):
    mock_fetch = mocker.patch("scmrepo.git.lfs.fetch")
    rev = scm.get_rev()

    with dvc.switch(rev):
        lfs_prefetch(dvc.dvcfs, ["foo"])
        mock_fetch.assert_not_called()

    tmp_dir.scm_gen(
        ".gitattributes", ".lfs filter=lfs diff=lfs merge=lfs -text", commit="init lfs"
    )
    rev = scm.get_rev()
    with dvc.switch(rev):
        lfs_prefetch(dvc.dvcfs, ["foo"])
        mock_fetch.assert_called_once()
