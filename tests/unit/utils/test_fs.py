import os
from unittest import TestCase

import dvc
import pytest
from dvc.system import System
from dvc.utils.compat import str
from dvc.utils.fs import (
    get_mtime_and_size,
    contains_symlink_up_to,
    BasePathNotInCheckedPathException,
    get_parent_dirs_up_to,
)
from mock import patch
from tests.basic_env import TestDir
from tests.utils import spy


class TestMtimeAndSize(TestDir):
    def test(self):
        file_time, file_size = get_mtime_and_size(self.DATA)
        dir_time, dir_size = get_mtime_and_size(self.DATA_DIR)

        actual_file_size = os.path.getsize(self.DATA)
        actual_dir_size = (
            os.path.getsize(self.DATA_DIR)
            + os.path.getsize(self.DATA)
            + os.path.getsize(self.DATA_SUB_DIR)
            + os.path.getsize(self.DATA_SUB)
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


@pytest.mark.parametrize(
    "path1, path2",
    [
        (
            os.path.join("non", "abs", "path"),
            os.path.join(os.sep, "full", "path"),
        ),
        (
            os.path.join(os.sep, "full", "path"),
            os.path.join("non", "abs", "path"),
        ),
    ],
)
def test_get_parent_dirs_up_to_should_raise_on_no_absolute(path1, path2):
    with pytest.raises(AssertionError):
        get_parent_dirs_up_to(path1, path2)


@pytest.mark.parametrize(
    "path1, path2, expected_dirs",
    [
        (
            os.path.join(os.sep, "non", "matching", "path"),
            os.path.join(os.sep, "other", "path"),
            [],
        ),
        (
            os.path.join(os.sep, "some", "long", "path"),
            os.path.join(os.sep, "some"),
            [
                os.path.join(os.sep, "some"),
                os.path.join(os.sep, "some", "long"),
                os.path.join(os.sep, "some", "long", "path"),
            ],
        ),
    ],
)
def test_get_parent_dirs_up_to(path1, path2, expected_dirs):
    result = get_parent_dirs_up_to(path1, path2)

    assert set(result) == set(expected_dirs)
