import os

import pytest

import dvc
from dvc.fs import system
from dvc.utils import relpath
from dvc.utils.fs import (
    BasePathNotInCheckedPathException,
    contains_symlink_up_to,
    makedirs,
    path_isin,
    remove,
)


def test_should_raise_exception_on_base_path_not_in_path():
    with pytest.raises(BasePathNotInCheckedPathException):
        contains_symlink_up_to(os.path.join("foo", "path"), "bar")


def test_should_return_true_on_symlink_in_path(mocker):
    mocker.patch.object(system, "is_symlink", return_value=True)
    base_path = "foo"
    path = os.path.join(base_path, "bar")
    assert contains_symlink_up_to(path, base_path)


def test_should_return_false_on_path_eq_to_base_path(mocker):
    mocker.patch.object(system, "is_symlink", return_value=False)
    path = "path"
    assert not contains_symlink_up_to(path, path)


def test_should_return_false_on_no_more_dirs_below_path(mocker):
    mocker.patch.object(system, "is_symlink", return_value=False)
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
        system,
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
    mocker.patch.object(system, "is_symlink", return_value=False)

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
