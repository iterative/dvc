import itertools
import os
import shutil

import pytest

from dvc.exceptions import DvcIgnoreInCollectedDirError
from dvc.ignore import DvcIgnore
from dvc.ignore import DvcIgnoreDirs
from dvc.ignore import DvcIgnoreFilter
from dvc.ignore import DvcIgnorePatterns
from dvc.scm.tree import WorkingTree
from dvc.utils.compat import cast_bytes
from dvc.utils.fs import get_mtime_and_size
from tests.basic_env import TestDvc
from tests.utils import to_posixpath


class TestDvcIgnore(TestDvc):
    def setUp(self):
        super(TestDvcIgnore, self).setUp()

    def _get_all_paths(self):

        paths = []
        for root, dirs, files in self.dvc.tree.walk(
            self.dvc.root_dir, dvcignore=self.dvc.dvcignore
        ):
            for dname in dirs:
                paths.append(os.path.join(root, dname))

            for fname in files:
                paths.append(os.path.join(root, fname))

        return paths

    def test_ignore_in_child_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_child_dir_unicode(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "wb") as fobj:
            fobj.write(cast_bytes(self.UNICODE, "utf-8"))

        forbidden_path = os.path.join(self.dvc.root_dir, self.UNICODE)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)

    def test_ignore_in_parent_dir(self):
        ignore_file = os.path.join(self.dvc.root_dir, DvcIgnore.DVCIGNORE_FILE)
        with open(ignore_file, "w") as fobj:
            fobj.write("data_dir/data")

        os.chdir(self.DATA_DIR)

        forbidden_path = os.path.join(self.dvc.root_dir, self.DATA)
        all_paths = self._get_all_paths()

        self.assertNotIn(forbidden_path, all_paths)


def test_metadata_unchanged_when_moving_ignored_file(dvc_repo, repo_dir):
    new_data_path = repo_dir.DATA_SUB + "_new"

    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(
        ignore_file,
        "\n".join(
            [to_posixpath(repo_dir.DATA_SUB), to_posixpath(new_data_path)]
        ),
    )

    mtime_sig, size = get_mtime_and_size(repo_dir.DATA_DIR, dvc_repo.dvcignore)

    shutil.move(repo_dir.DATA_SUB, new_data_path)

    new_mtime_sig, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, dvc_repo.dvcignore
    )

    assert new_mtime_sig == mtime_sig
    assert new_size == size


def test_mtime_changed_when_moving_non_ignored_file(dvc_repo, repo_dir):
    new_data_path = repo_dir.DATA_SUB + "_new"
    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR, dvc_repo.dvcignore)

    shutil.move(repo_dir.DATA_SUB, new_data_path)
    new_mtime, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, dvc_repo.dvcignore
    )

    assert new_mtime != mtime
    assert new_size == size


def test_metadata_unchanged_on_ignored_file_deletion(dvc_repo, repo_dir):
    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(ignore_file, to_posixpath(repo_dir.DATA_SUB))

    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR, dvc_repo.dvcignore)

    os.remove(repo_dir.DATA_SUB)
    new_mtime, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, dvc_repo.dvcignore
    )

    assert new_mtime == mtime
    assert new_size == size


def test_metadata_changed_on_non_ignored_file_deletion(dvc_repo, repo_dir):
    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR, dvc_repo.dvcignore)

    os.remove(repo_dir.DATA_SUB)
    new_mtime_sig, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, dvc_repo.dvcignore
    )

    assert new_mtime_sig != mtime
    assert new_size != size


def test_should_raise_on_dvcignore_in_out_dir(dvc_repo, repo_dir):
    ignore_file = os.path.join(repo_dir.DATA_DIR, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(ignore_file, "")

    with pytest.raises(DvcIgnoreInCollectedDirError):
        dvc_repo.add(repo_dir.DATA_DIR)


@pytest.mark.parametrize("dname", [TestDvc.DATA_DIR, TestDvc.DATA_SUB_DIR])
def test_ignore_collecting_dvcignores(repo_dir, dname):
    top_ignore_file = os.path.join(
        repo_dir.root_dir, os.path.dirname(dname), DvcIgnore.DVCIGNORE_FILE
    )
    repo_dir.create(top_ignore_file, os.path.basename(dname))

    ignore_file = os.path.join(
        repo_dir.root_dir, dname, DvcIgnore.DVCIGNORE_FILE
    )
    repo_dir.create(ignore_file, repo_dir.FOO)

    assert DvcIgnoreFilter(
        repo_dir.root_dir, WorkingTree(repo_dir.root_dir)
    ).ignores == {
        DvcIgnoreDirs([".git", ".hg", ".dvc"]),
        DvcIgnorePatterns(top_ignore_file, WorkingTree(repo_dir.root_dir)),
    }


def test_ignore_on_branch(git, dvc_repo, repo_dir):
    dvc_repo.add(repo_dir.DATA_DIR)
    dvc_repo.scm.commit("add data dir")

    branch_name = "branch_one"
    dvc_repo.scm.checkout(branch_name, create_new=True)

    repo_dir.create(DvcIgnore.DVCIGNORE_FILE, to_posixpath(repo_dir.DATA_SUB))
    dvc_repo.scm.add([DvcIgnore.DVCIGNORE_FILE])
    git.index.commit("add ignore")

    dvc_repo.scm.checkout("master")

    git_tree = dvc_repo.scm.get_tree(branch_name)
    branch_data_files = set(
        itertools.chain.from_iterable(
            [
                files
                for _, _, files in dvc_repo.tree.walk(
                    repo_dir.DATA_DIR,
                    dvcignore=DvcIgnoreFilter(repo_dir.root_dir, git_tree),
                )
            ]
        )
    )
    assert branch_data_files == {"data"}
