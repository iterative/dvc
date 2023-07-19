import os
import posixpath
import shutil

import pytest

from dvc.fs import localfs
from dvc.fs.dvc import DVCFileSystem
from dvc.testing.tmp_dir import make_subrepo
from dvc_data.hashfile.build import build
from dvc_data.hashfile.hash_info import HashInfo


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DVCFileSystem(repo=dvc)
    assert fs.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = DVCFileSystem(repo=dvc)
    with fs.open("foo", "r") as fobj:
        assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = DVCFileSystem(repo=dvc)
    with fs.open("file", "r") as fobj:
        assert fobj.read() == "something"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    fs = DVCFileSystem(repo=dvc)
    with fs.open("file", "r") as fobj:
        assert fobj.read() == "file"


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
        fs = DVCFileSystem(repo=dvc)
        with fs.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "datafile": "data",
            "datadir": {
                "foo": "foo",
                "bar": "bar",
            },
            "subdir": {
                "baz": "baz",
                "data": {
                    "abc": "abc",
                    "xyz": "xyz",
                },
            },
        },
    )

    fs = DVCFileSystem(repo=dvc)
    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert not fs.isdvc("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")
    assert not fs.isdvc("datafile")

    dvc.add(
        [
            "datadir",
            "datafile",
            os.path.join("subdir", "baz"),
            os.path.join("subdir", "data"),
        ]
    )
    shutil.rmtree(tmp_dir / "datadir")
    shutil.rmtree(tmp_dir / "subdir" / "data")
    (tmp_dir / "datafile").unlink()
    (tmp_dir / "subdir" / "baz").unlink()

    fs = DVCFileSystem(repo=dvc)
    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert fs.isdvc("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")
    assert fs.isdvc("datafile")

    assert fs.isdir("subdir")
    assert not fs.isfile("subdir")
    assert not fs.isdvc("subdir")
    assert fs.isfile("subdir/baz")
    assert fs.isdir("subdir/data")


def test_exists_isdir_isfile_dirty(tmp_dir, dvc):
    tmp_dir.dvc_gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    fs = DVCFileSystem(repo=dvc)
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    assert fs.exists("datafile")
    assert fs.exists("datadir")
    assert fs.exists("datadir/foo")
    assert fs.isfile("datafile")
    assert not fs.isfile("datadir")
    assert fs.isfile("datadir/foo")
    assert not fs.isdir("datafile")
    assert fs.isdir("datadir")
    assert not fs.isdir("datadir/foo")

    # NOTE: creating file instead of dir and dir instead of file
    tmp_dir.gen({"datadir": "data", "datafile": {"foo": "foo", "bar": "bar"}})
    assert fs.exists("datafile")
    assert fs.exists("datadir")
    assert not fs.exists("datadir/foo")
    assert fs.exists("datafile/foo")
    assert not fs.isfile("datafile")
    assert fs.isfile("datadir")
    assert not fs.isfile("datadir/foo")
    assert fs.isfile("datafile/foo")
    assert fs.isdir("datafile")
    assert not fs.isdir("datadir")
    assert not fs.isdir("datadir/foo")
    assert not fs.isdir("datafile/foo")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    fs = DVCFileSystem(repo=dvc)
    assert fs.isdir("dir")
    assert not fs.isfile("dir")


def test_ls_dirty(tmp_dir, dvc):
    tmp_dir.dvc_gen({"data": "data"})
    (tmp_dir / "data").unlink()

    tmp_dir.gen({"data": {"foo": "foo", "bar": "bar"}})

    fs = DVCFileSystem(repo=dvc)
    assert set(fs.ls("data")) == {"data/foo", "data/bar"}


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                "dir/subdir1/foo1.dvc",
                "dir/subdir1/bar1.dvc",
                "dir/subdir2/foo2.dvc",
            ],
        ),
    ],
)
def test_walk(tmp_dir, dvc, dvcfiles, extra_expected):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
            }
        }
    )
    dvc.add(localfs.find("dir"))
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    fs = DVCFileSystem(repo=dvc)

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
    for root, dirs, files in fs.walk("dir", dvcfiles=dvcfiles):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    expected += extra_expected
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_dirty(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {
            "dir": {
                "foo": "foo",
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
            }
        }
    )
    tmp_dir.gen({"dir": {"bar": "bar", "subdir3": {"foo3": "foo3"}}})
    (tmp_dir / "dir" / "foo").unlink()

    fs = DVCFileSystem(repo=dvc)
    expected = [
        "dir/subdir1",
        "dir/subdir2",
        "dir/subdir3",
        "dir/subdir1/foo1",
        "dir/subdir1/bar1",
        "dir/subdir2/foo2",
        "dir/subdir3/foo3",
        "dir/bar",
        "dir/foo",
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_dirty_cached_dir(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}}, commit="add data")
    (tmp_dir / "data" / "foo").unlink()

    fs = DVCFileSystem(repo=dvc)

    actual = []
    for root, dirs, files in fs.walk("data"):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    expected = ["data/foo", "data/bar"]
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_mixed_dir(tmp_dir, scm, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc.add(os.path.join("dir", "foo"))
    tmp_dir.scm.add(
        [
            os.path.join("dir", "bar"),
            os.path.join("dir", ".gitignore"),
            os.path.join("dir", "foo.dvc"),
        ]
    )
    tmp_dir.scm.commit("add dir")

    fs = DVCFileSystem(repo=dvc)

    expected = [
        "dir/foo",
        "dir/bar",
        "dir/.gitignore",
    ]
    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_missing(tmp_dir, dvc):
    fs = DVCFileSystem(repo=dvc)

    for _ in fs.walk("dir"):
        pass


def test_walk_not_a_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo")
    fs = DVCFileSystem(repo=dvc)

    for _ in fs.walk("foo"):
        pass


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar", "dir": {"baz": "baz"}})
    dvc.add("foo")
    dvc.add("dir")
    fs = DVCFileSystem(repo=dvc)
    assert fs.isdvc("foo")
    assert not fs.isdvc("bar")
    assert fs.isdvc("dir")
    assert fs.isdvc("dir/baz")
    assert fs.isdvc("dir/baz", recursive=True)


def test_subrepos(tmp_dir, scm, dvc, mocker):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse DVCFileSystem"}},
        commit="dir/repo.txt",
    )

    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    for repo in [subrepo1, subrepo2]:
        make_subrepo(repo, scm)

    with subrepo1.chdir():
        subrepo1.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
    with subrepo2.chdir():
        subrepo2.dvc_gen({"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR")

    dvc._reset()
    fs = DVCFileSystem(repo=dvc, subrepos=True)

    def assert_fs_belongs_to_repo(ret_val):
        method = fs.fs._get_repo

        def f(*args, **kwargs):
            r = method(*args, **kwargs)
            assert r.root_dir == ret_val.root_dir
            return r

        return f

    mock_subrepo1 = mocker.patch.object(
        fs.fs, "_get_repo", side_effect=assert_fs_belongs_to_repo(subrepo1.dvc)
    )
    assert fs.exists("dir/repo/foo") is True
    assert fs.exists("dir/repo/bar") is False

    assert fs.isfile("dir/repo/foo") is True
    assert fs.isfile("dir/repo/dir1/bar") is True
    assert fs.isfile("dir/repo/dir1") is False

    assert fs.isdir("dir/repo/dir1") is True
    assert fs.isdir("dir/repo/dir1/bar") is False
    assert fs.isdvc("dir/repo/foo") is True
    mocker.stop(mock_subrepo1)

    mock_subrepo2 = mocker.patch.object(
        fs.fs, "_get_repo", side_effect=assert_fs_belongs_to_repo(subrepo2.dvc)
    )
    assert fs.exists("dir/repo2/lorem") is True
    assert fs.exists("dir/repo2/ipsum") is False

    assert fs.isfile("dir/repo2/lorem") is True
    assert fs.isfile("dir/repo2/dir2/ipsum") is True
    assert fs.isfile("dir/repo2/dir2") is False

    assert fs.isdir("dir/repo2/dir2") is True
    assert fs.isdir("dir/repo2/dir2/ipsum") is False
    assert fs.isdvc("dir/repo2/lorem") is True
    mocker.stop(mock_subrepo2)


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                "dir/repo/foo.dvc",
                "dir/repo/.dvcignore",
                "dir/repo/dir1.dvc",
                "dir/repo2/.dvcignore",
                "dir/repo2/lorem.dvc",
                "dir/repo2/dir2.dvc",
            ],
        ),
    ],
)
def test_subrepo_walk(tmp_dir, scm, dvc, dvcfiles, extra_expected):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse DVCFileSystem"}},
        commit="dir/repo.txt",
    )

    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    subdirs = [subrepo1, subrepo2]
    for dir_ in subdirs:
        make_subrepo(dir_, scm)

    with subrepo1.chdir():
        subrepo1.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
    with subrepo2.chdir():
        subrepo2.dvc_gen({"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR")

    # using fs that does not have dvcignore
    dvc._reset()
    fs = DVCFileSystem(repo=dvc)
    expected = [
        "dir/repo",
        "dir/repo.txt",
        "dir/repo2",
        "dir/repo/.gitignore",
        "dir/repo/foo",
        "dir/repo/dir1",
        "dir/repo/dir1/bar",
        "dir/repo2/.gitignore",
        "dir/repo2/lorem",
        "dir/repo2/dir2",
        "dir/repo2/dir2/ipsum",
    ]

    actual = []
    for root, dirs, files in fs.walk(
        "dir",
        dvcfiles=dvcfiles,
        ignore_subrepos=False,
    ):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    expected += extra_expected
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_dvcfs_no_subrepos(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse DVCFileSystem"}},
        commit="dir/repo.txt",
    )
    tmp_dir.dvc_gen({"lorem": "lorem"}, commit="add foo")

    subrepo = tmp_dir / "dir" / "repo"
    make_subrepo(subrepo, scm)
    with subrepo.chdir():
        subrepo.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
        subrepo.scm_gen({"ipsum": "ipsum"}, commit="BAR")

    # using fs that does not have dvcignore
    dvc._reset()
    fs = DVCFileSystem(repo=dvc)
    expected = [
        "/.dvcignore",
        "/.gitignore",
        "/lorem",
        "/lorem.dvc",
        "/dir",
        "/dir/repo.txt",
    ]

    actual = []
    for root, dirs, files in fs.walk("/", dvcfiles=True):
        for entry in dirs + files:
            actual.append(posixpath.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)

    assert fs.isfile("lorem") is True
    assert fs.isfile("dir/repo/foo") is False
    assert fs.isdir("dir/repo") is False
    assert fs.isdir("dir") is True

    assert fs.isdvc("lorem") is True
    assert fs.isdvc("dir/repo/dir1") is False

    assert fs.exists("dir/repo.txt") is True
    assert fs.exists("repo/ipsum") is False


def test_get_hash_cached_file(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen({"foo": "foo"})
    fs = DVCFileSystem(repo=dvc)
    expected = "acbd18db4cc2f85cedef654fccc4a4d8"
    assert fs.info("foo").get("md5") is None
    _, _, obj = build(dvc.cache.local, "foo", fs, "md5")
    assert obj.hash_info == HashInfo("md5", expected)
    (tmp_dir / "foo").unlink()
    assert fs.info("foo")["md5"] == expected


def test_get_hash_cached_dir(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}})
    fs = DVCFileSystem(repo=dvc)
    expected = "8761c4e9acad696bee718615e23e22db.dir"
    assert fs.info("dir").get("md5") is None
    _, _, obj = build(dvc.cache.local, "dir", fs, "md5")
    assert obj.hash_info == HashInfo("md5", "8761c4e9acad696bee718615e23e22db.dir")

    shutil.rmtree(tmp_dir / "dir")
    assert fs.info("dir")["md5"] == expected
    _, _, obj = build(dvc.cache.local, "dir", fs, "md5")
    assert obj.hash_info == HashInfo("md5", "8761c4e9acad696bee718615e23e22db.dir")


def test_get_hash_cached_granular(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}})
    fs = DVCFileSystem(repo=dvc)
    subdir = "dir/subdir"
    assert fs.info(subdir).get("md5") is None
    _, _, obj = build(dvc.cache.local, subdir, fs, "md5")
    assert obj.hash_info == HashInfo("md5", "af314506f1622d107e0ed3f14ec1a3b5.dir")
    assert fs.info(posixpath.join(subdir, "data")).get("md5") is None
    _, _, obj = build(dvc.cache.local, posixpath.join(subdir, "data"), fs, "md5")
    assert obj.hash_info == HashInfo("md5", "8d777f385d3dfec8815d20f7496026dc")
    (tmp_dir / "dir" / "subdir" / "data").unlink()
    assert (
        fs.info(posixpath.join(subdir, "data"))["md5"]
        == "8d777f385d3dfec8815d20f7496026dc"
    )


def test_get_hash_mixed_dir(tmp_dir, scm, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    tmp_dir.dvc.add(os.path.join("dir", "foo"))
    tmp_dir.scm.add(
        [
            os.path.join("dir", "bar"),
            os.path.join("dir", ".gitignore"),
            os.path.join("dir", "foo.dvc"),
        ]
    )
    tmp_dir.scm.commit("add dir")

    fs = DVCFileSystem(repo=dvc)
    _, _, obj = build(dvc.cache.local, "dir", fs, "md5")
    if os.name == "nt":
        expected_hash = "0d2086760aea091f1504eafc8843bb18.dir"
    else:
        expected_hash = "e1d9e8eae5374860ae025ec84cfd85c7.dir"
    assert obj.hash_info == HashInfo("md5", expected_hash)


def test_get_hash_dirty_file(tmp_dir, dvc):
    from dvc_data.hashfile import check
    from dvc_data.hashfile.hash import hash_file

    tmp_dir.dvc_gen("file", "file")
    file_hash_info = HashInfo("md5", "8c7dd922ad47494fc02c388e12c00eac")

    (tmp_dir / "file").write_text("something")
    something_hash_info = HashInfo("md5", "437b930db84b8079c2dd804a71936b5f")

    # file is modified in workspace
    # hash_file(file) should return workspace hash, not DVC cached hash
    fs = DVCFileSystem(repo=dvc)
    assert fs.info("file").get("md5") is None
    staging, _, obj = build(dvc.cache.local, "file", fs, "md5")
    assert obj.hash_info == something_hash_info
    check(staging, obj)

    # hash_file(file) should return DVC cached hash
    (tmp_dir / "file").unlink()
    assert fs.info("file")["md5"] == file_hash_info.value
    _, hash_info = hash_file("file", fs, "md5", state=dvc.state)
    assert hash_info == file_hash_info

    # tmp_dir/file can be built even though it is missing in workspace since
    # repofs will use the DVC cached hash (and refer to the local cache object)
    _, _, obj = build(dvc.cache.local, "file", fs, "md5")
    assert obj.hash_info == file_hash_info


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")

    fs = DVCFileSystem(repo=dvc)
    _, meta, obj = build(dvc.cache.local, "dir", fs, "md5")
    assert obj.hash_info == HashInfo("md5", "ba75a2162ca9c29acecb7957105a0bc2.dir")
    assert meta.nfiles == 3


@pytest.mark.parametrize("traverse_subrepos", [True, False])
def test_walk_nested_subrepos(tmp_dir, dvc, scm, traverse_subrepos):
    # generate a dvc and fs structure, with suffix based on repo's basename
    def fs_structure(suffix):
        return {
            f"foo-{suffix}": f"foo-{suffix}",
            f"dir-{suffix}": {f"bar-{suffix}": f"bar-{suffix}"},
        }

    def dvc_structure(suffix):
        return {
            f"lorem-{suffix}": f"lorem-{suffix}",
            f"dvc-{suffix}": {f"ipsum-{suffix}": f"ipsum-{suffix}"},
        }

    paths = ["subrepo1", "subrepo2", os.path.join("subrepo1", "subrepo3")]
    subrepos = [tmp_dir / path for path in paths]
    for repo_dir in subrepos:
        make_subrepo(repo_dir, scm)

    extras = {".gitignore"}  # these files are always there
    expected = {}
    for repo_dir in [*subrepos, tmp_dir]:
        base = os.path.basename(repo_dir)
        scm_files = fs_structure(base)
        dvc_files = dvc_structure(base)
        with repo_dir.chdir():
            repo_dir.scm_gen(scm_files, commit=f"git add in {repo_dir}")
            repo_dir.dvc_gen(dvc_files, commit=f"dvc add in {repo_dir}")

        if traverse_subrepos or repo_dir == tmp_dir:
            repo_dir_path = (
                "/" + repo_dir.relative_to(tmp_dir).as_posix()
                if repo_dir != tmp_dir
                else "/"
            )
            expected[repo_dir_path] = set(scm_files.keys() | dvc_files.keys() | extras)
            # files inside a dvc directory
            expected[posixpath.join(repo_dir_path, f"dvc-{base}")] = {f"ipsum-{base}"}
            # files inside a git directory
            expected[posixpath.join(repo_dir_path, f"dir-{base}")] = {f"bar-{base}"}

    if traverse_subrepos:
        # update subrepos
        expected["/"].update(["subrepo1", "subrepo2"])
        expected["/subrepo1"].add("subrepo3")

    actual = {}
    fs = DVCFileSystem(repo=dvc)
    for root, dirs, files in fs.walk("/", ignore_subrepos=not traverse_subrepos):
        actual[root] = set(dirs + files)
    assert expected == actual
