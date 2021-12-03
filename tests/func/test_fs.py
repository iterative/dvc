import io
import os
from operator import itemgetter
from os.path import join

import fsspec
import pytest

from dvc.fs import get_cloud_fs
from dvc.fs.local import LocalFileSystem
from dvc.repo import Repo
from dvc.scm import SCM
from tests.basic_env import TestDir, TestGit, TestGitSubmodule


class TestLocalFileSystem(TestDir):
    def setUp(self):
        super().setUp()
        self.fs = LocalFileSystem()

    def test_open(self):
        with self.fs.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with self.fs.open(self.UNICODE, encoding="utf-8") as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)

    def test_exists(self):
        self.assertTrue(self.fs.exists(self.FOO))
        self.assertTrue(self.fs.exists(self.UNICODE))
        self.assertFalse(self.fs.exists("not-existing-file"))

    def test_isdir(self):
        self.assertTrue(self.fs.isdir(self.DATA_DIR))
        self.assertFalse(self.fs.isdir(self.FOO))
        self.assertFalse(self.fs.isdir("not-existing-file"))

    def test_isfile(self):
        self.assertTrue(self.fs.isfile(self.FOO))
        self.assertFalse(self.fs.isfile(self.DATA_DIR))
        self.assertFalse(self.fs.isfile("not-existing-file"))


class GitFileSystemTests:
    # pylint: disable=no-member
    def test_open(self):
        self.scm.add([self.FOO, self.UNICODE, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        with fs.open(self.FOO, mode="r", encoding="utf-8") as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with fs.open(self.UNICODE, mode="r", encoding="utf-8") as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)
        with self.assertRaises(IOError):
            fs.open("not-existing-file")
        with self.assertRaises(IOError):
            fs.open(self.DATA_DIR)

    def test_exists(self):
        fs = self.scm.get_fs("master")
        self.assertFalse(fs.exists(self.FOO))
        self.assertFalse(fs.exists(self.UNICODE))
        self.assertFalse(fs.exists(self.DATA_DIR))
        self.scm.add([self.FOO, self.UNICODE, self.DATA])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.exists(self.FOO))
        self.assertTrue(fs.exists(self.UNICODE))
        self.assertTrue(fs.exists(self.DATA_DIR))
        self.assertFalse(fs.exists("non-existing-file"))

    def test_isdir(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.isdir(self.DATA_DIR))
        self.assertFalse(fs.isdir(self.FOO))
        self.assertFalse(fs.isdir("non-existing-file"))

    def test_isfile(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        fs = self.scm.get_fs("master")
        self.assertTrue(fs.isfile(self.FOO))
        self.assertFalse(fs.isfile(self.DATA_DIR))
        self.assertFalse(fs.isfile("not-existing-file"))


class TestGitFileSystem(TestGit, GitFileSystemTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)


class TestGitSubmoduleFileSystem(TestGitSubmodule, GitFileSystemTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)
        self._pushd(self._root_dir)


class AssertWalkEqualMixin:
    def assertWalkEqual(self, actual, expected, msg=None):
        def convert_to_sets(walk_results):
            return [
                (root, set(dirs), set(nondirs))
                for root, dirs, nondirs in walk_results
            ]

        self.assertEqual(
            convert_to_sets(actual), convert_to_sets(expected), msg=msg
        )


class TestWalkInNoSCM(AssertWalkEqualMixin, TestDir):
    def test(self):
        fs = LocalFileSystem()
        self.assertWalkEqual(
            fs.walk(self._root_dir),
            [
                (
                    self._root_dir,
                    ["data_dir"],
                    ["code.py", "bar", "тест", "foo"],
                ),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], ["data"]),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )

    def test_subdir(self):
        fs = LocalFileSystem()
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )


class TestWalkInGit(AssertWalkEqualMixin, TestGit):
    def test_nobranch(self):
        fs = LocalFileSystem(url=self._root_dir)
        walk_result = []
        for root, dirs, files in fs.walk("."):
            dirs[:] = [i for i in dirs if i != ".git"]
            walk_result.append((root, dirs, files))
        self.assertWalkEqual(
            walk_result,
            [
                (".", ["data_dir"], ["bar", "тест", "code.py", "foo"]),
                (join("data_dir"), ["data_sub_dir"], ["data"]),
                (join("data_dir", "data_sub_dir"), [], ["data_sub"]),
            ],
        )
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )

    def test_branch(self):
        scm = SCM(self._root_dir)
        scm.add([self.DATA_SUB_DIR])
        scm.commit("add data_dir/data_sub_dir/data_sub")
        fs = scm.get_fs("master")
        self.assertWalkEqual(
            fs.walk("."),
            [
                (self._root_dir, ["data_dir"], ["code.py"]),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], []),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )
        self.assertWalkEqual(
            fs.walk(join("data_dir", "data_sub_dir")),
            [
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                )
            ],
        )


def test_cleanfs_subrepo(tmp_dir, dvc, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen({"foo": "foo", "dir": {"bar": "bar"}})

    path = subrepo_dir.fs_path

    assert dvc.fs.exists(dvc.fs.path.join(path, "foo"))
    assert dvc.fs.isfile(dvc.fs.path.join(path, "foo"))
    assert dvc.fs.exists(dvc.fs.path.join(path, "dir"))
    assert dvc.fs.isdir(dvc.fs.path.join(path, "dir"))

    assert subrepo.fs.exists(subrepo.fs.path.join(path, "foo"))
    assert subrepo.fs.isfile(subrepo.fs.path.join(path, "foo"))
    assert subrepo.fs.exists(subrepo.fs.path.join(path, "dir"))
    assert subrepo.fs.isdir(subrepo.fs.path.join(path, "dir"))


def test_walk_dont_ignore_subrepos(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="add foo")
    subrepo_dir = tmp_dir / "subdir"
    subrepo_dir.mkdir()
    with subrepo_dir.chdir():
        Repo.init(subdir=True)
    scm.add(["subdir"])
    scm.commit("Add subrepo")

    dvc_fs = dvc.fs
    dvc._reset()
    scm_fs = scm.get_fs("HEAD")
    path = os.fspath(tmp_dir)
    get_dirs = itemgetter(1)

    assert set(get_dirs(next(dvc_fs.walk(path)))) == {".dvc", "subdir", ".git"}
    assert set(get_dirs(next(scm_fs.walk(path)))) == {".dvc", "subdir"}


def test_fs_getsize(dvc, cloud):
    cloud.gen({"data": {"foo": "foo"}, "baz": "baz baz"})
    cls, config, path = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    assert fs.getsize(fs.path.join(path, "baz")) == 7
    assert fs.getsize(fs.path.join(path, "data", "foo")) == 3


def test_fs_upload_fobj(dvc, tmp_dir, cloud):
    tmp_dir.gen("foo", "foo")
    cls, config, path = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    from_info = tmp_dir / "foo"
    to_info = fs.path.join(path, "foo")

    with open(from_info, "rb") as stream:
        fs.upload_fobj(stream, to_info)

    assert fs.exists(to_info)
    with fs.open(to_info, "rb") as stream:
        assert stream.read() == b"foo"


def test_fs_makedirs_on_upload_and_copy(dvc, cloud):
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    with io.BytesIO(b"foo") as stream:
        fs.upload(stream, (cloud / "dir" / "foo").fs_path)

    assert fs.isdir((cloud / "dir").fs_path)
    assert fs.exists((cloud / "dir" / "foo").fs_path)

    fs.makedirs((cloud / "dir2").fs_path)
    fs.copy((cloud / "dir" / "foo").fs_path, (cloud / "dir2" / "foo").fs_path)
    assert fs.isdir((cloud / "dir2").fs_path)
    assert fs.exists((cloud / "dir2" / "foo").fs_path)


def test_upload_callback(tmp_dir, dvc, cloud):
    tmp_dir.gen("foo", "foo")
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)
    expected_size = os.path.getsize(tmp_dir / "foo")

    callback = fsspec.Callback()
    fs.upload(
        (tmp_dir / "foo").fs_path,
        (cloud / "foo").fs_path,
        callback=callback,
    )

    assert callback.size == expected_size
    assert callback.value == expected_size


def test_download_callback(tmp_dir, dvc, cloud, local_cloud):
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    (tmp_dir / "to_upload").write_text("foo")
    fs.upload((tmp_dir / "to_upload").fs_path, (cloud / "foo").fs_path)
    expected_size = fs.getsize((cloud / "foo").fs_path)

    callback = fsspec.Callback()
    fs.download_file(
        (cloud / "foo").fs_path,
        (tmp_dir / "foo").fs_path,
        callback=callback,
    )

    assert callback.size == expected_size
    assert callback.value == expected_size
    assert (tmp_dir / "foo").read_text() == "foo"


def test_download_dir_callback(tmp_dir, dvc, cloud):
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)
    cloud.gen({"dir": {"foo": "foo", "bar": "bar"}})

    callback = fsspec.Callback()
    fs.download(
        (cloud / "dir").fs_path, (tmp_dir / "dir").fs_path, callback=callback
    )

    assert callback.size == 2
    assert callback.value == 2
    assert (tmp_dir / "dir").read_text() == {"foo": "foo", "bar": "bar"}


@pytest.mark.parametrize("fs_type", ["git", "dvc"])
def test_download_callbacks_on_dvc_git_fs(tmp_dir, dvc, scm, fs_type):
    from dvc.fs.git import GitFileSystem

    gen = tmp_dir.scm_gen if fs_type == "git" else tmp_dir.dvc_gen
    gen({"dir": {"foo": "foo", "bar": "bar"}, "file": "file"}, commit="gen")

    fs = dvc.dvcfs if fs_type == "dvc" else GitFileSystem(scm=scm, rev="HEAD")

    callback = fsspec.Callback()
    fs.download_file(
        (tmp_dir / "file").fs_path,
        (tmp_dir / "file2").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "file")
    assert (tmp_dir / "file2").read_text() == "file"
    assert callback.size == size
    assert callback.value == size

    callback = fsspec.Callback()
    fs.download(
        (tmp_dir / "dir").fs_path,
        (tmp_dir / "dir2").fs_path,
        callback=callback,
    )

    assert (tmp_dir / "dir2").read_text() == {"foo": "foo", "bar": "bar"}
    assert callback.size == 2
    assert callback.value == 2


def test_callback_on_repo_fs(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen({"dir": {"bar": "bar"}}, commit="dvc")
    tmp_dir.scm_gen({"dir": {"foo": "foo"}}, commit="git")

    fs = dvc.repo_fs

    callback = fsspec.Callback()
    fs.download(
        (tmp_dir / "dir").fs_path,
        (tmp_dir / "dir2").fs_path,
        callback=callback,
    )

    assert (tmp_dir / "dir2").read_text() == {"foo": "foo", "bar": "bar"}
    assert callback.size == 2
    assert callback.value == 2

    callback = fsspec.Callback()
    fs.download(
        (tmp_dir / "dir" / "foo").fs_path,
        (tmp_dir / "foo").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "foo")
    assert (tmp_dir / "foo").read_text() == "foo"
    assert callback.size == size
    assert callback.value == size

    callback = fsspec.Callback()
    fs.download(
        (tmp_dir / "dir" / "bar").fs_path,
        (tmp_dir / "bar").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "bar")
    assert (tmp_dir / "bar").read_text() == "bar"
    assert callback.size == size
    assert callback.value == size
