import os
import filecmp
import shutil

import pytest
from tests.utils import trees_equal

from dvc.stage import Stage
from dvc.exceptions import DownloadError


def test_import(repo_dir, git, dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_import_dir(repo_dir, git, dvc_repo, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)
    assert git.git.check_ignore(dst)


def test_import_rev(repo_dir, git, dvc_repo, erepo):
    src = "version"
    dst = src

    dvc_repo.imp(erepo.root_dir, src, dst, rev="branch")

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    with open(dst, "r+") as fobj:
        assert fobj.read() == "branch"
    assert git.git.check_ignore(dst)


def test_pull_imported_stage(dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    dst_stage = Stage.load(dvc_repo, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    os.remove(dst)
    os.remove(dst_cache)

    dvc_repo.pull(["foo_imported.dvc"])

    assert os.path.isfile(dst)
    assert os.path.isfile(dst_cache)


def test_download_error_pulling_imported_stage(dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    dst_stage = Stage.load(dvc_repo, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    shutil.rmtree(erepo.root_dir)
    os.remove(dst)
    os.remove(dst_cache)

    with pytest.raises(DownloadError):
        dvc_repo.pull(["foo_imported.dvc"])
