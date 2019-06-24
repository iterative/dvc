import os
import shutil

from dvc.ignore import DvcIgnore, DvcIgnoreFileHandler
from dvc.utils.compat import cast_bytes
from dvc.utils.fs import get_mtime_and_size
from tests.basic_env import TestDvc


class TestDvcIgnore(TestDvc):
    def setUp(self):
        super(TestDvcIgnore, self).setUp()
        self.ignore_file_handler = DvcIgnoreFileHandler(self.dvc.tree)

    def _get_all_paths(self):

        paths = []
        ignore_file_handler = DvcIgnoreFileHandler(self.dvc.tree)
        for root, dirs, files in self.dvc.tree.walk(
            self.dvc.root_dir, ignore_file_handler=ignore_file_handler
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

    repo_dir.create(ignore_file, "\n".join([repo_dir.DATA_SUB, new_data_path]))

    ignore_handler = DvcIgnoreFileHandler(dvc_repo.tree)

    mtime_sig, size = get_mtime_and_size(repo_dir.DATA_DIR, ignore_handler)

    shutil.move(repo_dir.DATA_SUB, new_data_path)

    new_mtime_sig, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, ignore_handler
    )

    assert new_mtime_sig == mtime_sig
    assert new_size == size


def test_mtime_changed_when_moving_non_ignored_file(dvc_repo, repo_dir):
    new_data_path = repo_dir.DATA_SUB + "_new"
    ignore_handler = DvcIgnoreFileHandler(dvc_repo.tree)
    mtime, size = get_mtime_and_size(repo_dir.DATA_DIR, ignore_handler)

    shutil.move(repo_dir.DATA_SUB, new_data_path)
    new_mtime, new_size = get_mtime_and_size(repo_dir.DATA_DIR, ignore_handler)

    assert new_mtime != mtime
    assert new_size == size


def test_metadata_unchanged_on_ignored_file_deletion(dvc_repo, repo_dir):
    ignore_file = os.path.join(dvc_repo.root_dir, DvcIgnore.DVCIGNORE_FILE)
    repo_dir.create(ignore_file, repo_dir.DATA_SUB)
    ignore_handler = DvcIgnoreFileHandler(dvc_repo.tree)
    mtime_sig, size = get_mtime_and_size(repo_dir.DATA_DIR, ignore_handler)

    os.remove(repo_dir.DATA_SUB)
    new_mtime_sig, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, ignore_handler
    )

    assert new_mtime_sig == mtime_sig
    assert new_size == size


def test_metadata_changed_on_non_ignored_file_deletion(dvc_repo, repo_dir):
    ignore_handler = DvcIgnoreFileHandler(dvc_repo.tree)
    mtime_sig, size = get_mtime_and_size(repo_dir.DATA_DIR, ignore_handler)

    os.remove(repo_dir.DATA_SUB)
    new_mtime_sig, new_size = get_mtime_and_size(
        repo_dir.DATA_DIR, ignore_handler
    )

    assert new_mtime_sig != mtime_sig
    assert new_size != size
