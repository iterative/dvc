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


def test_import_git_file(erepo_dir, tmp_dir, dvc, scm):
    src = "some_file"
    dst = "some_file_imported"

    erepo_dir.scm_gen({src: "hello"}, commit="add a regular file")

    tmp_dir.dvc.imp(str(erepo_dir), src, dst)

    assert (tmp_dir / dst).exists()
    assert os.path.isfile(str(tmp_dir / dst))
    assert filecmp.cmp(str(erepo_dir / src), str(tmp_dir / dst), shallow=False)
    assert tmp_dir.scm.repo.git.check_ignore(str(tmp_dir / dst))


def test_status_imported_git_file(erepo_dir, tmp_dir, dvc, scm):
    src = "some_file"
    dst = "some_file_imported"

    erepo_dir.scm_gen({src: "hello"}, commit="add a regular file")

    tmp_dir.dvc.imp(str(erepo_dir), src, dst)
    tmp_dir.dvc.status([dst + ".dvc"])


def test_import_git_dir(erepo_dir, tmp_dir, dvc, scm):
    src = "some_directory"
    dst = "some_directory_imported"

    erepo_dir.scm_gen({src: {"file.txt": "hello"}}, commit="add a dir")

    tmp_dir.dvc.imp(str(erepo_dir), src, dst)

    assert (tmp_dir / dst).exists()
    assert os.path.isdir(str(tmp_dir / dst))
    trees_equal(str(erepo_dir / src), str(tmp_dir / dst))
    assert tmp_dir.scm.repo.git.check_ignore(str(tmp_dir / dst))


def test_import_dir(git, dvc_repo, erepo):
    src = erepo.DATA_DIR
    dst = erepo.DATA_DIR + "_imported"

    dvc_repo.imp(erepo.root_dir, src, dst)

    assert os.path.exists(dst)
    assert os.path.isdir(dst)
    trees_equal(src, dst)
    assert git.git.check_ignore(dst)


def test_import_non_cached(erepo_dir, tmp_dir, dvc, scm):
    src = "non_cached_output"
    dst = src + "_imported"

    erepo_dir.dvc.run(
        cmd="echo hello > {}".format(src),
        outs_no_cache=[src],
        cwd=str(erepo_dir),
    )

    erepo_dir.scm.add([str(erepo_dir / src)])
    erepo_dir.scm.commit("add a non-cached output")

    tmp_dir.dvc.imp(str(erepo_dir), src, dst)

    assert (tmp_dir / dst).exists()
    assert os.path.isfile(str(tmp_dir / dst))
    assert filecmp.cmp(str(erepo_dir / src), str(tmp_dir / dst), shallow=False)
    assert tmp_dir.scm.repo.git.check_ignore(dst)


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


def test_import_non_existing(erepo_dir, tmp_dir, dvc):
    with pytest.raises(PathMissingError):
        tmp_dir.dvc.imp(str(erepo_dir), "invalid_output")
    # https://github.com/iterative/dvc/pull/2837#discussion_r352123053
    with pytest.raises(NoOutputInExternalRepoError):
        tmp_dir.dvc.imp(str(erepo_dir), "/root/", "root")
