import os
from unittest import TestCase

import pytest
from mock import patch

import dvc
from dvc.ignore import DvcIgnoreFilter
from dvc.path_info import PathInfo
from dvc.scm.tree import WorkingTree
from dvc.system import System
from dvc.utils import relpath
from dvc.utils.compat import str
from dvc.utils.fs import BasePathNotInCheckedPathException
from dvc.utils.fs import contains_symlink_up_to
from dvc.utils.fs import get_inode
from dvc.utils.fs import get_mtime_and_size
from dvc.utils.fs import move
from dvc.utils.fs import path_isin, remove
from tests.basic_env import TestDir
from tests.utils import spy


class TestMtimeAndSize(TestDir):
    def test(self):
        dvcignore = DvcIgnoreFilter(self.root_dir, WorkingTree(self.root_dir))
        file_time, file_size = get_mtime_and_size(self.DATA, dvcignore)
        dir_time, dir_size = get_mtime_and_size(self.DATA_DIR, dvcignore)

        actual_file_size = os.path.getsize(self.DATA)
        actual_dir_size = os.path.getsize(self.DATA) + os.path.getsize(
            self.DATA_SUB
        )

        self.assertIs(type(file_time), str)
        self.assertIs(type(file_size), str)
        self.assertEqual(file_size, str(actual_file_size))
        self.assertIs(type(dir_time), str)
        self.assertIs(type(dir_size), str)
        self.assertEqual(dir_size, str(actual_dir_size))


class TestContainsLink(TestCase):
    def test_should_raise_exception_on_base_path_not_in_path(self):
        with self.assertRaises(BasePathNotInCheckedPathException):
            contains_symlink_up_to(os.path.join("foo", "path"), "bar")

    @patch.object(System, "is_symlink", return_value=True)
    def test_should_return_true_on_symlink_in_path(self, _):
        base_path = "foo"
        path = os.path.join(base_path, "bar")
        self.assertTrue(contains_symlink_up_to(path, base_path))

    @patch.object(System, "is_symlink", return_value=False)
    def test_should_return_false_on_path_eq_to_base_path(self, _):
        path = "path"
        self.assertFalse(contains_symlink_up_to(path, path))

    @patch.object(System, "is_symlink", return_value=False)
    @patch.object(os.path, "dirname", side_effect=lambda arg: arg)
    def test_should_return_false_on_no_more_dirs_below_path(
        self, dirname_patch, _
    ):
        self.assertFalse(
            contains_symlink_up_to(os.path.join("foo", "path"), "foo")
        )
        dirname_patch.assert_called_once()

    @patch.object(System, "is_symlink", return_value=False)
    def test_should_call_recursive_on_no_condition_matched(self, _):
        contains_symlink_spy = spy(contains_symlink_up_to)
        with patch.object(
            dvc.utils.fs, "contains_symlink_up_to", contains_symlink_spy
        ):

            # call from full path to match contains_symlink_spy patch path
            self.assertFalse(
                dvc.utils.fs.contains_symlink_up_to(
                    os.path.join("foo", "path"), "foo"
                )
            )
            self.assertEqual(2, contains_symlink_spy.mock.call_count)

    @patch.object(System, "is_symlink", return_value=True)
    def test_should_return_false_when_base_path_is_symlink(self, _):
        base_path = "foo"
        target_path = os.path.join(base_path, "bar")

        def base_path_is_symlink(path):
            if path == base_path:
                return True
            return False

        with patch.object(
            System, "is_symlink", side_effect=base_path_is_symlink
        ):
            self.assertFalse(contains_symlink_up_to(target_path, base_path))

    def test_path_object_and_str_are_valid_arg_types(self):
        base_path = "foo"
        target_path = os.path.join(base_path, "bar")
        self.assertFalse(contains_symlink_up_to(target_path, base_path))
        self.assertFalse(
            contains_symlink_up_to(PathInfo(target_path), PathInfo(base_path))
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_relpath_windows_different_drives():
    path1 = os.path.join("A:", os.sep, "some", "path")
    path2 = os.path.join("B:", os.sep, "other", "path")
    assert relpath(path1, path2) == path1

    info1, info2 = PathInfo(path1), PathInfo(path2)
    rel_info = relpath(info1, info2)
    assert isinstance(rel_info, str)
    assert rel_info == path1


def test_get_inode(repo_dir):
    path = repo_dir.FOO
    path_info = PathInfo(path)
    assert get_inode(path) == get_inode(path_info)


@pytest.mark.parametrize("path", [TestDir.DATA, TestDir.DATA_DIR])
def test_path_object_and_str_are_valid_types_get_mtime_and_size(
    path, repo_dir
):
    dvcignore = DvcIgnoreFilter(
        repo_dir.root_dir, WorkingTree(repo_dir.root_dir)
    )
    time, size = get_mtime_and_size(path, dvcignore)
    object_time, object_size = get_mtime_and_size(PathInfo(path), dvcignore)
    assert time == object_time
    assert size == object_size


def test_move(repo_dir):
    src = repo_dir.FOO
    src_info = PathInfo(repo_dir.BAR)
    dest = os.path.join("some", "directory")
    dest_info = PathInfo(os.path.join("some", "path-like", "directory"))

    os.makedirs(dest)
    assert len(os.listdir(dest)) == 0
    move(src, dest)
    assert not os.path.isfile(src)
    assert len(os.listdir(dest)) == 1

    os.makedirs(dest_info.fspath)
    assert len(os.listdir(dest_info.fspath)) == 0
    move(src_info, dest_info)
    assert not os.path.isfile(src_info.fspath)
    assert len(os.listdir(dest_info.fspath)) == 1


def test_remove(repo_dir):
    path = repo_dir.FOO
    path_info = PathInfo(repo_dir.BAR)

    remove(path)
    assert not os.path.isfile(path)

    remove(path_info)
    assert not os.path.isfile(path_info.fspath)


def test_path_isin_positive():
    child = os.path.join("path", "to", "folder")

    assert path_isin(child, os.path.join("path", "to", ""))
    assert path_isin(child, os.path.join("path", "to"))
    assert path_isin(child, os.path.join("path", ""))
    assert path_isin(child, os.path.join("path"))


def test_path_isin_on_same_path():
    path = os.path.join("path", "to", "folder")
    path_with_sep = os.path.join(path, "")

    assert not path_isin(path, path)
    assert not path_isin(path, path_with_sep)
    assert not path_isin(path_with_sep, path)
    assert not path_isin(path_with_sep, path_with_sep)


def test_path_isin_on_common_substring_path():
    path1 = os.path.join("path", "to", "folder1")
    path2 = os.path.join("path", "to", "folder")

    assert not path_isin(path1, path2)


def test_path_isin_accepts_pathinfo():
    child = os.path.join("path", "to", "folder")
    parent = PathInfo(child) / ".."

    assert path_isin(child, parent)
    assert not path_isin(parent, child)


def test_path_isin_with_absolute_path():
    parent = os.path.abspath("path")
    child = os.path.join(parent, "to", "folder")

    assert path_isin(child, parent)
