from __future__ import unicode_literals

import filecmp
import os
import shutil

import pytest
from mock import patch

from dvc.config import Config
from dvc.exceptions import DownloadError
from dvc.stage import Stage
from dvc.system import System
from dvc.utils import makedirs
from tests.utils import trees_equal


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


def test_cache_type_is_properly_overridden(repo_dir, git, dvc_repo, erepo):
    erepo.dvc.config.set(
        Config.SECTION_CACHE, Config.SECTION_CACHE_TYPE, "symlink"
    )
    erepo.dvc.scm.add([erepo.dvc.config.config_file])
    erepo.dvc.scm.commit("set source repo cache type to symlinks")

    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert not System.is_symlink(dst)
    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_pull_imported_directory_stage(dvc_repo, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"
    stage_file = dst + ".dvc"

    dvc_repo.imp(erepo.root_dir, src, dst)

    shutil.rmtree(dst)
    shutil.rmtree(dvc_repo.cache.local.cache_dir)

    dvc_repo.pull([stage_file])

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)


def test_download_error_pulling_imported_stage(dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    dst_stage = Stage.load(dvc_repo, "foo_imported.dvc")
    dst_cache = dst_stage.outs[0].cache_path

    os.remove(dst)
    os.remove(dst_cache)

    with patch(
        "dvc.remote.RemoteLOCAL._download", side_effect=Exception
    ), pytest.raises(DownloadError):
        dvc_repo.pull(["foo_imported.dvc"])


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_to_dir(dname, repo_dir, dvc_repo, erepo):
    src = erepo.FOO

    makedirs(dname, exist_ok=True)

    stage = dvc_repo.imp(erepo.root_dir, src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert filecmp.cmp(repo_dir.FOO, dst, shallow=False)


def test_pull_non_workspace(git, dvc_repo, erepo):
    src = "version"
    dst = src

    stage = dvc_repo.imp(erepo.root_dir, src, dst, rev="branch")
    dvc_repo.scm.add([stage.relpath])
    dvc_repo.scm.commit("imported branch")
    dvc_repo.scm.tag("ref-to-branch")

    # Ovewrite via import
    dvc_repo.imp(erepo.root_dir, src, dst, rev="master")

    os.remove(stage.outs[0].cache_path)
    dvc_repo.fetch(all_tags=True)
    assert os.path.exists(stage.outs[0].cache_path)
