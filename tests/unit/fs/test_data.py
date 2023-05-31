import posixpath
import shutil

import pytest

import dvc_data
from dvc.fs import localfs
from dvc.fs.data import DataFileSystem
from dvc.utils.fs import remove
from dvc_data.hashfile.build import build
from dvc_data.hashfile.hash_info import HashInfo


@pytest.mark.parametrize(
    "path, key",
    [
        ("", ()),
        (".", ()),
        ("/", ()),
        ("foo", ("foo",)),
        ("dir/foo", ("dir", "foo")),
    ],
)
def test_get_key(tmp_dir, dvc, path, key):
    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert fs.fs._get_key(path) == key


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert fs.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DataFileSystem(index=dvc.index.data["repo"])
    with fs.open("foo", "r") as fobj:
        assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DataFileSystem(index=dvc.index.data["repo"])
    with fs.open("file", "r") as fobj:
        # NOTE: Unlike DVCFileSystem, DataFileSystem should not
        # be affected by a dirty workspace.
        assert fobj.read() == "file"


def test_open_no_remote(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").unlink()
    remove(dvc.cache.local.path)

    fs = DataFileSystem(index=dvc.index.data["repo"])
    with pytest.raises(FileNotFoundError):
        with fs.open("file", "r"):
            pass


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    fs = DataFileSystem(index=dvc.index.data["repo"])
    # NOTE: Unlike DVCFileSystem, DataFileSystem should not
    # be affected by a dirty workspace.
    with pytest.raises(FileNotFoundError):
        with fs.open("file", "r"):
            pass


def test_open_in_history(tmp_dir, scm, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foo")

    tmp_dir.gen("foo", "foofoo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foofoo")

    with dvc.switch("HEAD~1"):
        fs = DataFileSystem(index=dvc.index.data["repo"])
        with fs.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert not fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert not fs.isfile("datafile")

    dvc.add(["datadir", "datafile"])
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert fs.isdir("dir")
    assert not fs.isfile("dir")


def test_walk(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
                "foo": "foo",
                "bar": "bar",
            }
        }
    )

    dvc.add(localfs.find("dir"))
    fs = DataFileSystem(index=dvc.index.data["repo"])

    expected = [
        "dir/subdir1",
        "dir/subdir2",
        "dir/subdir1/foo1",
        "dir/subdir1/bar1",
        "dir/subdir2/foo2",
        "dir/foo",
        "dir/bar",
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_dir(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
                "foo": "foo",
                "bar": "bar",
            }
        }
    )

    dvc.add("dir")
    fs = DataFileSystem(index=dvc.index.data["repo"])

    expected = [
        "dir/subdir1",
        "dir/subdir2",
        "dir/subdir1/foo1",
        "dir/subdir1/bar1",
        "dir/subdir2/foo2",
        "dir/foo",
        "dir/bar",
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_missing(tmp_dir, dvc):
    fs = DataFileSystem(index=dvc.index.data["repo"])

    for _ in fs.walk("dir"):
        pass


def test_walk_not_a_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    fs = DataFileSystem(index=dvc.index.data["repo"])

    for _ in fs.walk("foo"):
        pass


def test_get_hash_file(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    fs = DataFileSystem(index=dvc.index.data["repo"])
    assert fs.info("foo")["md5"] == "acbd18db4cc2f85cedef654fccc4a4d8"


def test_get_hash_dir(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}})
    fs = DataFileSystem(index=dvc.index.data["repo"])
    hash_file_spy = mocker.spy(dvc_data.hashfile.hash, "hash_file")
    assert fs.info("dir")["md5"] == "8761c4e9acad696bee718615e23e22db.dir"
    assert not hash_file_spy.called


def test_get_hash_granular(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}})
    fs = DataFileSystem(index=dvc.index.data["repo"])
    subdir = "dir/subdir"
    assert fs.info(subdir).get("md5") is None
    _, _, obj = build(dvc.cache.local, subdir, fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", "af314506f1622d107e0ed3f14ec1a3b5.dir")
    data = posixpath.join(subdir, "data")
    assert fs.info(data)["md5"] == "8d777f385d3dfec8815d20f7496026dc"
    _, _, obj = build(dvc.cache.local, data, fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", "8d777f385d3dfec8815d20f7496026dc")


def test_get_hash_dirty_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DataFileSystem(index=dvc.index.data["repo"])
    expected = "8c7dd922ad47494fc02c388e12c00eac"
    assert fs.info("file").get("md5") == expected
    _, _, obj = build(dvc.cache.local, "file", fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", expected)


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")

    fs = DataFileSystem(index=dvc.index.data["repo"])
    expected = "5ea40360f5b4ec688df672a4db9c17d1.dir"
    assert fs.info("dir").get("md5") == expected
    _, _, obj = build(dvc.cache.local, "dir", fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", expected)
