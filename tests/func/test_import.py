from __future__ import unicode_literals

import filecmp
import os
import shutil

import pytest
from mock import patch

from dvc.config import Config
from dvc.exceptions import DownloadError
from dvc.exceptions import PathMissingError
from dvc.exceptions import NoOutputInExternalRepoError
from dvc.stage import Stage
from dvc.system import System
from dvc.utils import makedirs
from tests.utils import trees_equal


def test_import(git, dvc_repo, erepo):
    src = erepo.FOO
    dst = erepo.FOO + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_import_git_file(git, dvc_repo, erepo):
    src = "some_file"
    dst = "some_file_imported"

    src_path = os.path.join(erepo.root_dir, src)
    erepo.create(src_path, "hello")
    erepo.dvc.scm.add([src_path])
    erepo.dvc.scm.commit("add a regular file")

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(src_path, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_import_git_dir(git, dvc_repo, erepo):
    src = "some_directory"
    dst = "some_directory_imported"

    src_file_path = os.path.join(erepo.root_dir, src, "file.txt")
    erepo.create(src_file_path, "hello")
    erepo.dvc.scm.add([src_file_path])
    erepo.dvc.scm.commit("add a regular dir")

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(os.path.join(erepo.root_dir, src), dst)
    assert git.git.check_ignore(dst)


def test_import_dir(git, dvc_repo, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)
    assert git.git.check_ignore(dst)


def test_import_non_cached(git, dvc_repo, erepo):
    src = "non_cached_output"
    dst = src + "_imported"

    erepo.dvc.run(
        cmd="echo hello > {}".format(src),
        outs_no_cache=[src],
        cwd=erepo.root_dir,
    )

    src_path = os.path.join(erepo.root_dir, src)
    erepo.dvc.scm.add([src_path])
    erepo.dvc.scm.commit("add a non-cached output")

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isfile(dst)
    assert filecmp.cmp(src_path, dst, shallow=False)
    assert git.git.check_ignore(dst)


def test_import_rev(git, dvc_repo, erepo):
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


def test_cache_type_is_properly_overridden(git, dvc_repo, erepo):
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
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)
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
def test_import_to_dir(dname, dvc_repo, erepo):
    src = erepo.FOO

    makedirs(dname, exist_ok=True)

    stage = dvc_repo.imp(erepo.root_dir, src, dname)

    dst = os.path.join(dname, os.path.basename(src))

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert filecmp.cmp(erepo.FOO, dst, shallow=False)


def test_pull_non_workspace(git, dvc_repo, erepo):
    src = "version"
    dst = src

    stage = dvc_repo.imp(erepo.root_dir, src, dst, rev="branch")
    dvc_repo.scm.add([stage.relpath])
    dvc_repo.scm.commit("imported branch")
    dvc_repo.scm.tag("ref-to-branch")

    # Overwrite via import
    dvc_repo.imp(erepo.root_dir, src, dst, rev="master")

    os.remove(stage.outs[0].cache_path)
    dvc_repo.fetch(all_tags=True)
    assert os.path.exists(stage.outs[0].cache_path)


def test_import_non_existing(dvc_repo, erepo):
    with pytest.raises(PathMissingError):
        dvc_repo.imp(erepo.root_dir, "invalid_output")
    # https://github.com/iterative/dvc/pull/2837#discussion_r352123053
    with pytest.raises(NoOutputInExternalRepoError):
        dvc_repo.imp(erepo.root_dir, "/root/", "root")
