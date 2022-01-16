import filecmp
import os

import pytest

import dvc
from dvc.fs.local import LocalFileSystem
from dvc.system import System
from dvc.utils import relpath
from dvc.utils.fs import (
    BasePathNotInCheckedPathException,
    contains_symlink_up_to,
    copy_fobj_to_file,
    copyfile,
    get_inode,
    get_mtime_and_size,
    makedirs,
    move,
    path_isin,
    remove,
    walk_files,
)


def test_mtime_and_size(tmp_dir):
    tmp_dir.gen({"dir/data": "data", "dir/subdir/subdata": "subdata"})
    fs = LocalFileSystem(url=tmp_dir)
    file_time, file_size = get_mtime_and_size("dir/data", fs)
    dir_time, dir_size = get_mtime_and_size("dir", fs)

    actual_file_size = os.path.getsize("dir/data")
    actual_dir_size = os.path.getsize("dir/data") + os.path.getsize(
        "dir/subdir/subdata"
    )

    assert isinstance(file_time, str)
    assert isinstance(file_size, int)
    assert file_size == actual_file_size
    assert isinstance(dir_time, str)
    assert isinstance(dir_size, int)
    assert dir_size == actual_dir_size


def test_should_raise_exception_on_base_path_not_in_path():
    with pytest.raises(BasePathNotInCheckedPathException):
        contains_symlink_up_to(os.path.join("foo", "path"), "bar")


def test_should_return_true_on_symlink_in_path(mocker):
    mocker.patch.object(System, "is_symlink", return_value=True)
    base_path = "foo"
    path = os.path.join(base_path, "bar")
    assert contains_symlink_up_to(path, base_path)


def test_should_return_false_on_path_eq_to_base_path(mocker):
    mocker.patch.object(System, "is_symlink", return_value=False)
    path = "path"
    assert not contains_symlink_up_to(path, path)


def test_should_return_false_on_no_more_dirs_below_path(mocker):
    mocker.patch.object(System, "is_symlink", return_value=False)
    dirname_patch = mocker.patch.object(
        os.path, "dirname", side_effect=lambda arg: arg
    )
    assert not contains_symlink_up_to(os.path.join("foo", "path"), "foo")
    dirname_patch.assert_called_once()


def test_should_return_false_when_base_path_is_symlink(mocker):
    base_path = "foo"
    target_path = os.path.join(base_path, "bar")

    def base_path_is_symlink(path):
        if path == base_path:
            return True
        return False

    mocker.patch.object(
        System,
        "is_symlink",
        return_value=True,
        side_effect=base_path_is_symlink,
    )
    assert not contains_symlink_up_to(target_path, base_path)


def test_path_object_and_str_are_valid_arg_types():
    base_path = "foo"
    target_path = os.path.join(base_path, "bar")
    assert not contains_symlink_up_to(target_path, base_path)
    assert not contains_symlink_up_to(target_path, base_path)


def test_should_call_recursive_on_no_condition_matched(mocker):
    mocker.patch.object(System, "is_symlink", return_value=False)

    contains_symlink_spy = mocker.spy(dvc.utils.fs, "contains_symlink_up_to")

    # call from full path to match contains_symlink_spy patch path
    assert not dvc.utils.fs.contains_symlink_up_to(
        os.path.join("foo", "path"), "foo"
    )
    assert contains_symlink_spy.mock.call_count == 2


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_relpath_windows_different_drives():
    path1 = os.path.join("A:", os.sep, "some", "path")
    path2 = os.path.join("B:", os.sep, "other", "path")
    assert relpath(path1, path2) == path1

    rel = relpath(path1, path2)
    assert isinstance(rel, str)
    assert rel == path1


def test_get_inode(tmp_dir):
    tmp_dir.gen("foo", "foo content")

    assert get_inode("foo") == get_inode("foo")


def test_path_object_and_str_are_valid_types_get_mtime_and_size(tmp_dir):
    tmp_dir.gen(
        {"dir": {"dir_file": "dir file content"}, "file": "file_content"}
    )
    fs = LocalFileSystem(url=os.fspath(tmp_dir))

    time, size = get_mtime_and_size("dir", fs)
    object_time, object_size = get_mtime_and_size("dir", fs)
    assert time == object_time
    assert size == object_size

    time, size = get_mtime_and_size("file", fs)
    object_time, object_size = get_mtime_and_size("file", fs)
    assert time == object_time
    assert size == object_size


def test_move(tmp_dir):
    tmp_dir.gen({"foo": "foo content"})
    src = "foo"
    dest = os.path.join("some", "directory")

    os.makedirs(dest)
    assert len(os.listdir(dest)) == 0
    move(src, dest)
    assert not os.path.isfile(src)
    assert len(os.listdir(dest)) == 1


def test_remove(tmp_dir):
    tmp_dir.gen({"foo": "foo content"})
    path = "foo"

    remove(path)
    assert not os.path.isfile(path)


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


def test_path_isin_with_absolute_path():
    parent = os.path.abspath("path")
    child = os.path.join(parent, "to", "folder")

    assert path_isin(child, parent)


def test_path_isin_case_sensitive():
    child = os.path.join("path", "to", "folder")
    parent = os.path.join("PATH", "TO")

    assert path_isin(child, parent) == (os.name == "nt")


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_contains_symlink_case_sensitive_win():
    child = os.path.join("path", "to", "folder")
    parent = os.path.join("PATH", "TO")
    assert contains_symlink_up_to(child, parent) is False


@pytest.mark.skipif(os.name == "nt", reason="Posix specific")
def test_contains_symlink_case_sensitive_posix():
    child = os.path.join("path", "to", "folder")
    parent = os.path.join("PATH", "TO")
    with pytest.raises(BasePathNotInCheckedPathException):
        contains_symlink_up_to(child, parent)


def test_makedirs(tmp_dir):
    path = os.path.join(tmp_dir, "directory")

    makedirs(path)
    assert os.path.isdir(path)


@pytest.mark.parametrize("path", ["file", "dir"])
def test_copyfile(path, tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo content",
            "file": "file content",
            "dir": {},
        }
    )
    src = "foo"
    dest = path

    copyfile(src, dest)
    if os.path.isdir(dest):
        assert filecmp.cmp(
            src, os.path.join(dest, os.path.basename(src)), shallow=False
        )
    else:
        assert filecmp.cmp(src, dest, shallow=False)


def test_copy_fobj_to_file(tmp_dir):
    tmp_dir.gen({"foo": "foo content"})
    src = tmp_dir / "foo"
    dest = "path"

    with open(src, "rb") as fobj:
        copy_fobj_to_file(fobj, dest)
    assert filecmp.cmp(src, dest)


def test_walk_files(tmp_dir):
    assert list(walk_files(".")) == list(walk_files(tmp_dir))
