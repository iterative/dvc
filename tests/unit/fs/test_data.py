import posixpath
import shutil

import pytest

import dvc_data
from dvc.config import NoRemoteError
from dvc.fs.data import DataFileSystem
from dvc.utils.fs import remove
from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.stage import stage


@pytest.mark.parametrize(
    "path, key",
    [
        ("", ("repo",)),
        (".", ("repo",)),
        ("/", ("repo",)),
        ("foo", ("repo", "foo")),
        ("dir/foo", ("repo", "dir", "foo")),
    ],
)
def test_get_key(tmp_dir, dvc, path, key):
    fs = DataFileSystem(repo=dvc)
    assert fs.fs._get_key(path) == key


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DataFileSystem(repo=dvc)
    assert fs.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DataFileSystem(repo=dvc)
    with fs.open("foo", "r") as fobj:
        assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DataFileSystem(repo=dvc)
    with fs.open("file", "r") as fobj:
        # NOTE: Unlike DvcFileSystem, DataFileSystem should not
        # be affected by a dirty workspace.
        assert fobj.read() == "file"


def test_open_no_remote(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").unlink()
    remove(dvc.odb.local.cache_dir)

    fs = DataFileSystem(repo=dvc)
    with pytest.raises(FileNotFoundError) as exc_info:
        with fs.open("file", "r"):
            pass
    assert isinstance(exc_info.value.__cause__, NoRemoteError)


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    fs = DataFileSystem(repo=dvc)
    # NOTE: Unlike DvcFileSystem, DataFileSystem should not
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

    for rev in dvc.brancher(revs=["HEAD~1"]):
        if rev == "workspace":
            continue

        fs = DataFileSystem(repo=dvc)
        with fs.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    fs = DataFileSystem(repo=dvc)
    assert not fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert not fs.isfile("datafile")

    dvc.add(["datadir", "datafile"])
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    fs = DataFileSystem(repo=dvc)
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

    dvc.add("dir", recursive=True)
    fs = DataFileSystem(repo=dvc)

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
    fs = DataFileSystem(repo=dvc)

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
    fs = DataFileSystem(repo=dvc)

    for _ in fs.walk("dir"):
        pass


def test_walk_not_a_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    fs = DataFileSystem(repo=dvc)

    for _ in fs.walk("foo"):
        pass


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    dvc.add("foo")
    fs = DataFileSystem(repo=dvc)
    assert fs.isdvc("foo")
    assert not fs.isdvc("bar")


def test_get_hash_file(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    fs = DataFileSystem(repo=dvc)
    assert fs.info("foo")["md5"] == "acbd18db4cc2f85cedef654fccc4a4d8"


def test_get_hash_dir(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = DataFileSystem(repo=dvc)
    hash_file_spy = mocker.spy(dvc_data.hashfile.hash, "hash_file")
    assert fs.info("dir")["md5"] == "8761c4e9acad696bee718615e23e22db.dir"
    assert not hash_file_spy.called


def test_get_hash_granular(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = DataFileSystem(repo=dvc)
    subdir = "dir/subdir"
    assert fs.info(subdir).get("md5") is None
    _, _, obj = stage(dvc.odb.local, subdir, fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo(
        "md5", "af314506f1622d107e0ed3f14ec1a3b5.dir"
    )
    data = posixpath.join(subdir, "data")
    assert fs.info(data)["md5"] == "8d777f385d3dfec8815d20f7496026dc"
    _, _, obj = stage(dvc.odb.local, data, fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", "8d777f385d3dfec8815d20f7496026dc")


def test_get_hash_dirty_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DataFileSystem(repo=dvc)
    expected = "8c7dd922ad47494fc02c388e12c00eac"
    assert fs.info("file").get("md5") == expected
    _, _, obj = stage(dvc.odb.local, "file", fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", expected)


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")

    fs = DataFileSystem(repo=dvc)
    expected = "5ea40360f5b4ec688df672a4db9c17d1.dir"
    assert fs.info("dir").get("md5") == expected
    _, _, obj = stage(dvc.odb.local, "dir", fs, "md5", dry_run=True)
    assert obj.hash_info == HashInfo("md5", expected)
