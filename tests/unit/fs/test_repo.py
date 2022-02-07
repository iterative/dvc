import os
import shutil
from unittest import mock

import pytest

from dvc.data.stage import stage
from dvc.fs.repo import RepoFileSystem
from dvc.hash_info import HashInfo
from tests.utils import clean_staging


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = RepoFileSystem(repo=dvc)
    assert fs.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    fs = RepoFileSystem(repo=dvc)
    with fs.open((tmp_dir / "foo").fs_path, "r") as fobj:
        assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    fs = RepoFileSystem(repo=dvc)
    with fs.open((tmp_dir / "file").fs_path, "r") as fobj:
        assert fobj.read() == "something"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    fs = RepoFileSystem(repo=dvc)
    with fs.open((tmp_dir / "file").fs_path, "r") as fobj:
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

    for rev in dvc.brancher(revs=["HEAD~1"]):
        if rev == "workspace":
            continue

        fs = RepoFileSystem(repo=dvc)
        with fs.open((tmp_dir / "foo").fs_path, "r") as fobj:
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

    fs = RepoFileSystem(repo=dvc)
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

    assert fs.isdir("datadir")
    assert not fs.isfile("datadir")
    assert fs.isdvc("datadir")
    assert not fs.isdir("datafile")
    assert fs.isfile("datafile")
    assert fs.isdvc("datafile")

    assert fs.isdir("subdir")
    assert not fs.isfile("subdir")
    assert not fs.isdvc("subdir")
    assert fs.isfile(os.path.join("subdir", "baz"))
    assert fs.isdir(os.path.join("subdir", "data"))


def test_exists_isdir_isfile_dirty(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}}
    )

    fs = RepoFileSystem(repo=dvc)
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    root = tmp_dir
    assert fs.exists(root / "datafile")
    assert fs.exists(root / "datadir")
    assert fs.exists(root / "datadir" / "foo")
    assert fs.isfile(root / "datafile")
    assert not fs.isfile(root / "datadir")
    assert fs.isfile(root / "datadir" / "foo")
    assert not fs.isdir(root / "datafile")
    assert fs.isdir(root / "datadir")
    assert not fs.isdir(root / "datadir" / "foo")

    # NOTE: creating file instead of dir and dir instead of file
    tmp_dir.gen({"datadir": "data", "datafile": {"foo": "foo", "bar": "bar"}})
    assert fs.exists(root / "datafile")
    assert fs.exists(root / "datadir")
    assert not fs.exists(root / "datadir" / "foo")
    assert fs.exists(root / "datafile" / "foo")
    assert not fs.isfile(root / "datafile")
    assert fs.isfile(root / "datadir")
    assert not fs.isfile(root / "datadir" / "foo")
    assert fs.isfile(root / "datafile" / "foo")
    assert fs.isdir(root / "datafile")
    assert not fs.isdir(root / "datadir")
    assert not fs.isdir(root / "datadir" / "foo")
    assert not fs.isdir(root / "datafile" / "foo")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    fs = RepoFileSystem(repo=dvc)
    assert fs.isdir("dir")
    assert not fs.isfile("dir")


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                os.path.join("dir", "subdir1", "foo1.dvc"),
                os.path.join("dir", "subdir1", "bar1.dvc"),
                os.path.join("dir", "subdir2", "foo2.dvc"),
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
    dvc.add(str(tmp_dir / "dir"), recursive=True)
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    fs = RepoFileSystem(repo=dvc)

    expected = [
        os.path.join("dir", "subdir1"),
        os.path.join("dir", "subdir2"),
        os.path.join("dir", "subdir1", "foo1"),
        os.path.join("dir", "subdir1", "bar1"),
        os.path.join("dir", "subdir2", "foo2"),
        os.path.join("dir", "foo"),
        os.path.join("dir", "bar"),
    ]

    actual = []
    for root, dirs, files in fs.walk("dir", dvcfiles=dvcfiles):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

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

    fs = RepoFileSystem(repo=dvc)
    expected = [
        os.path.join("dir", "subdir1"),
        os.path.join("dir", "subdir2"),
        os.path.join("dir", "subdir3"),
        os.path.join("dir", "subdir1", "foo1"),
        os.path.join("dir", "subdir1", "bar1"),
        os.path.join("dir", "subdir2", "foo2"),
        os.path.join("dir", "subdir3", "foo3"),
        os.path.join("dir", "bar"),
    ]

    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_dirty_cached_dir(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}}, commit="add data")
    (tmp_dir / "data" / "foo").unlink()

    fs = RepoFileSystem(repo=dvc)

    data = tmp_dir / "data"

    actual = []
    for root, dirs, files in fs.walk(data):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert actual == [(data / "bar").fs_path]


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

    fs = RepoFileSystem(repo=dvc)

    expected = [
        os.path.join("dir", "foo"),
        os.path.join("dir", "bar"),
        os.path.join("dir", ".gitignore"),
    ]
    actual = []
    for root, dirs, files in fs.walk("dir"):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_onerror(tmp_dir, dvc):
    def onerror(exc):
        raise exc

    tmp_dir.dvc_gen("foo", "foo")
    fs = RepoFileSystem(repo=dvc)

    # path does not exist
    for _ in fs.walk("dir"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("dir", onerror=onerror):
            pass

    # path is not a directory
    for _ in fs.walk("foo"):
        pass
    with pytest.raises(OSError):
        for _ in fs.walk("foo", onerror=onerror):
            pass


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar", "dir": {"baz": "baz"}})
    dvc.add("foo")
    dvc.add("dir")
    fs = RepoFileSystem(repo=dvc)
    assert fs.isdvc("foo")
    assert not fs.isdvc("bar")
    assert fs.isdvc("dir")
    assert fs.isdvc(os.path.join("dir", "baz"))
    assert fs.isdvc(os.path.join("dir", "baz"), recursive=True)


def make_subrepo(dir_, scm, config=None):
    dir_.mkdir(parents=True, exist_ok=True)
    with dir_.chdir():
        dir_.scm = scm
        dir_.init(dvc=True, subdir=True)
        if config:
            dir_.add_remote(config=config)


def test_subrepos(tmp_dir, scm, dvc, mocker):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse RepoFileSystem"}},
        commit="dir/repo.txt",
    )

    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    for repo in [subrepo1, subrepo2]:
        make_subrepo(repo, scm)

    with subrepo1.chdir():
        subrepo1.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
    with subrepo2.chdir():
        subrepo2.dvc_gen(
            {"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR"
        )

    dvc._reset()
    fs = RepoFileSystem(repo=dvc, subrepos=True)

    def assert_fs_belongs_to_repo(ret_val):
        method = fs._get_repo

        def f(*args, **kwargs):
            r = method(*args, **kwargs)
            assert r.root_dir == ret_val.root_dir
            return r

        return f

    with mock.patch.object(
        fs, "_get_repo", side_effect=assert_fs_belongs_to_repo(subrepo1.dvc)
    ):
        assert fs.exists((subrepo1 / "foo").fs_path) is True
        assert fs.exists((subrepo1 / "bar").fs_path) is False

        assert fs.isfile((subrepo1 / "foo").fs_path) is True
        assert fs.isfile((subrepo1 / "dir1" / "bar").fs_path) is True
        assert fs.isfile((subrepo1 / "dir1").fs_path) is False

        assert fs.isdir((subrepo1 / "dir1").fs_path) is True
        assert fs.isdir((subrepo1 / "dir1" / "bar").fs_path) is False
        assert fs.isdvc((subrepo1 / "foo").fs_path) is True

    with mock.patch.object(
        fs, "_get_repo", side_effect=assert_fs_belongs_to_repo(subrepo2.dvc)
    ):
        assert fs.exists((subrepo2 / "lorem").fs_path) is True
        assert fs.exists((subrepo2 / "ipsum").fs_path) is False

        assert fs.isfile((subrepo2 / "lorem").fs_path) is True
        assert fs.isfile((subrepo2 / "dir2" / "ipsum").fs_path) is True
        assert fs.isfile((subrepo2 / "dir2").fs_path) is False

        assert fs.isdir((subrepo2 / "dir2").fs_path) is True
        assert fs.isdir((subrepo2 / "dir2" / "ipsum").fs_path) is False
        assert fs.isdvc((subrepo2 / "lorem").fs_path) is True


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                os.path.join("dir", "repo", "foo.dvc"),
                os.path.join("dir", "repo", ".dvcignore"),
                os.path.join("dir", "repo", "dir1.dvc"),
                os.path.join("dir", "repo2", ".dvcignore"),
                os.path.join("dir", "repo2", "lorem.dvc"),
                os.path.join("dir", "repo2", "dir2.dvc"),
            ],
        ),
    ],
)
def test_subrepo_walk(tmp_dir, scm, dvc, dvcfiles, extra_expected):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse RepoFileSystem"}},
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
        subrepo2.dvc_gen(
            {"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR"
        )

    # using fs that does not have dvcignore
    dvc._reset()
    fs = RepoFileSystem(repo=dvc)
    expected = [
        os.path.join("dir", "repo"),
        os.path.join("dir", "repo.txt"),
        os.path.join("dir", "repo2"),
        os.path.join("dir", "repo", ".gitignore"),
        os.path.join("dir", "repo", "foo"),
        os.path.join("dir", "repo", "dir1"),
        os.path.join("dir", "repo", "dir1", "bar"),
        os.path.join("dir", "repo2", ".gitignore"),
        os.path.join("dir", "repo2", "lorem"),
        os.path.join("dir", "repo2", "dir2"),
        os.path.join("dir", "repo2", "dir2", "ipsum"),
    ]

    actual = []
    for root, dirs, files in fs.walk(
        os.path.join(fs.root_dir, "dir"),
        dvcfiles=dvcfiles,
        ignore_subrepos=False,
    ):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    expected = [
        os.path.join(fs.root_dir, path) for path in expected + extra_expected
    ]
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_repo_fs_no_subrepos(tmp_dir, dvc, scm):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse RepoFileSystem"}},
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
    fs = RepoFileSystem(repo=dvc)
    expected = [
        tmp_dir / ".dvcignore",
        tmp_dir / ".gitignore",
        tmp_dir / "lorem",
        tmp_dir / "lorem.dvc",
        tmp_dir / "dir",
        tmp_dir / "dir" / "repo.txt",
    ]

    actual = []
    for root, dirs, files in fs.walk(tmp_dir.fs_path, dvcfiles=True):
        for entry in dirs + files:
            actual.append(os.path.normpath(os.path.join(root, entry)))

    expected = [str(path) for path in expected]
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)

    assert fs.isfile(tmp_dir / "lorem") is True
    assert fs.isfile(tmp_dir / "dir" / "repo" / "foo") is False
    assert fs.isdir(tmp_dir / "dir" / "repo") is False
    assert fs.isdir(tmp_dir / "dir") is True

    assert fs.isdvc(tmp_dir / "lorem") is True
    assert fs.isdvc(tmp_dir / "dir" / "repo" / "dir1") is False

    assert fs.exists(tmp_dir / "dir" / "repo.txt") is True
    assert fs.exists(tmp_dir / "repo" / "ipsum") is False


def test_get_hash_cached_file(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen({"foo": "foo"})
    fs = RepoFileSystem(repo=dvc)
    expected = "acbd18db4cc2f85cedef654fccc4a4d8"
    assert fs.info((tmp_dir / "foo").fs_path).get("md5") is None
    _, _, obj = stage(dvc.odb.local, (tmp_dir / "foo").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo("md5", expected)
    (tmp_dir / "foo").unlink()
    assert fs.info((tmp_dir / "foo").fs_path)["md5"] == expected


def test_get_hash_cached_dir(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = RepoFileSystem(repo=dvc)
    expected = "8761c4e9acad696bee718615e23e22db.dir"
    assert fs.info((tmp_dir / "dir").fs_path).get("md5") is None
    _, _, obj = stage(dvc.odb.local, (tmp_dir / "dir").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo(
        "md5", "8761c4e9acad696bee718615e23e22db.dir"
    )

    shutil.rmtree(tmp_dir / "dir")
    assert fs.info((tmp_dir / "dir").fs_path)["md5"] == expected
    _, _, obj = stage(dvc.odb.local, (tmp_dir / "dir").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo(
        "md5", "8761c4e9acad696bee718615e23e22db.dir"
    )


def test_get_hash_cached_granular(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    fs = RepoFileSystem(repo=dvc)
    subdir = tmp_dir / "dir" / "subdir"
    assert fs.info(subdir.fs_path).get("md5") is None
    _, _, obj = stage(dvc.odb.local, subdir.fs_path, fs, "md5")
    assert obj.hash_info == HashInfo(
        "md5", "af314506f1622d107e0ed3f14ec1a3b5.dir"
    )
    assert fs.info((subdir / "data").fs_path).get("md5") is None
    _, _, obj = stage(dvc.odb.local, (subdir / "data").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo("md5", "8d777f385d3dfec8815d20f7496026dc")
    (tmp_dir / "dir" / "subdir" / "data").unlink()
    assert (
        fs.info((subdir / "data").fs_path)["md5"]
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
    clean_staging()

    fs = RepoFileSystem(repo=dvc)
    _, _, obj = stage(dvc.odb.local, (tmp_dir / "dir").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo(
        "md5", "e1d9e8eae5374860ae025ec84cfd85c7.dir"
    )


def test_get_hash_dirty_file(tmp_dir, dvc):
    from dvc.data import check
    from dvc.data.stage import get_file_hash
    from dvc.objects.errors import ObjectFormatError

    tmp_dir.dvc_gen("file", "file")
    file_hash_info = HashInfo("md5", "8c7dd922ad47494fc02c388e12c00eac")

    (tmp_dir / "file").write_text("something")
    something_hash_info = HashInfo("md5", "437b930db84b8079c2dd804a71936b5f")

    clean_staging()

    # file is modified in workspace
    # get_file_hash(file) should return workspace hash, not DVC cached hash
    fs = RepoFileSystem(repo=dvc)
    assert fs.info((tmp_dir / "file").fs_path).get("md5") is None
    staging, _, obj = stage(
        dvc.odb.local, (tmp_dir / "file").fs_path, fs, "md5"
    )
    assert obj.hash_info == something_hash_info
    check(staging, obj)

    # file is removed in workspace
    # any staged object referring to modified workspace obj is now invalid
    (tmp_dir / "file").unlink()
    with pytest.raises(ObjectFormatError):
        check(staging, obj)

    # get_file_hash(file) should return DVC cached hash
    assert fs.info((tmp_dir / "file").fs_path)["md5"] == file_hash_info.value
    _, hash_info = get_file_hash(
        (tmp_dir / "file").fs_path, fs, "md5", state=dvc.state
    )
    assert hash_info == file_hash_info

    # tmp_dir/file can be staged even though it is missing in workspace since
    # repofs will use the DVC cached hash (and refer to the local cache object)
    _, _, obj = stage(dvc.odb.local, (tmp_dir / "file").fs_path, fs, "md5")
    assert obj.hash_info == file_hash_info


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")
    clean_staging()

    fs = RepoFileSystem(repo=dvc)
    _, meta, obj = stage(dvc.odb.local, (tmp_dir / "dir").fs_path, fs, "md5")
    assert obj.hash_info == HashInfo(
        "md5", "ba75a2162ca9c29acecb7957105a0bc2.dir"
    )
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
    for repo_dir in subrepos + [tmp_dir]:
        base = os.path.basename(repo_dir)
        scm_files = fs_structure(base)
        dvc_files = dvc_structure(base)
        with repo_dir.chdir():
            repo_dir.scm_gen(scm_files, commit=f"git add in {repo_dir}")
            repo_dir.dvc_gen(dvc_files, commit=f"dvc add in {repo_dir}")

        if traverse_subrepos or repo_dir == tmp_dir:
            expected[str(repo_dir)] = set(
                scm_files.keys() | dvc_files.keys() | extras
            )
            # files inside a dvc directory
            expected[str(repo_dir / f"dvc-{base}")] = {f"ipsum-{base}"}
            # files inside a git directory
            expected[str(repo_dir / f"dir-{base}")] = {f"bar-{base}"}

    if traverse_subrepos:
        # update subrepos
        expected[str(tmp_dir)].update(["subrepo1", "subrepo2"])
        expected[str(tmp_dir / "subrepo1")].add("subrepo3")

    actual = {}
    fs = RepoFileSystem(repo=dvc)
    for root, dirs, files in fs.walk(
        str(tmp_dir), ignore_subrepos=not traverse_subrepos
    ):
        actual[root] = set(dirs + files)
    assert expected == actual
