import os
from io import BytesIO
from operator import itemgetter
from os.path import join

import pytest

from dvc.fs import LocalFileSystem, get_cloud_fs
from dvc.fs.callbacks import DEFAULT_CALLBACK, Callback
from dvc.repo import Repo


def test_local_fs_open(tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem()

    with fs.open("foo", encoding="utf-8") as fobj:
        assert fobj.read() == "foo"
    with fs.open("тест", encoding="utf-8") as fobj:
        assert fobj.read() == "проверка"


def test_local_fs_exists(tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem()

    assert fs.exists("foo")
    assert fs.exists("тест")
    assert not fs.exists("not-existing-file")


def test_local_fs_isdir(tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem()

    assert fs.isdir("data_dir")
    assert not fs.isdir("foo")
    assert not fs.isdir("not-existing-file")


def test_local_fs_isfile(tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem()

    assert fs.isfile("foo")
    assert not fs.isfile("data_dir")
    assert not fs.isfile("not-existing-file")


def convert_to_sets(walk_results):
    return [
        (root, set(dirs), set(nondirs)) for root, dirs, nondirs in walk_results
    ]


def test_walk_no_scm(tmp_dir):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem()
    walk_results = fs.walk(str(tmp_dir))
    assert convert_to_sets(walk_results) == [
        (str(tmp_dir), {"data_dir"}, {"code.py", "bar", "тест", "foo"}),
        (str(tmp_dir / "data_dir"), {"data_sub_dir"}, {"data"}),
        (str(tmp_dir / "data_dir" / "data_sub_dir"), set(), {"data_sub"}),
    ]

    walk_results = fs.walk(join("data_dir", "data_sub_dir"))
    assert convert_to_sets(walk_results) == [
        (join("data_dir", "data_sub_dir"), set(), {"data_sub"}),
    ]


def test_walk_fs_with_git(tmp_dir, scm):
    tmp_dir.gen(
        {
            "foo": "foo",
            "bar": "bar",
            "тест": "проверка",
            "code.py": "import sys\nimport shutil\n"
            "shutil.copyfile(sys.argv[1], sys.argv[2])",
            "data_dir": {
                "data": "data",
                "data_sub_dir": {"data_sub": "data_sub"},
            },
        }
    )
    fs = LocalFileSystem(url=str(tmp_dir))
    walk_result = []
    for root, dirs, files in fs.walk("."):
        dirs[:] = [i for i in dirs if i != ".git"]
        walk_result.append((root, dirs, files))

    assert convert_to_sets(walk_result) == [
        (".", {"data_dir"}, {"bar", "тест", "code.py", "foo"}),
        (join("data_dir"), {"data_sub_dir"}, {"data"}),
        (join("data_dir", "data_sub_dir"), set(), {"data_sub"}),
    ]

    walk_result = fs.walk(join("data_dir", "data_sub_dir"))
    assert convert_to_sets(walk_result) == [
        (join("data_dir", "data_sub_dir"), set(), {"data_sub"})
    ]


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
    assert set(get_dirs(next(scm_fs.walk("/")))) == {".dvc", "subdir"}


def test_fs_size(dvc, cloud):
    cloud.gen({"data": {"foo": "foo"}, "baz": "baz baz"})
    cls, config, path = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)

    assert fs.size(fs.path.join(path, "baz")) == 7
    assert fs.size(fs.path.join(path, "data", "foo")) == 3


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
    with BytesIO(b"foo") as stream:
        fs.put_file(stream, (cloud / "dir" / "foo").fs_path)

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

    callback = Callback()
    fs.put_file(
        (tmp_dir / "foo").fs_path,
        (cloud / "foo").fs_path,
        callback=callback,
    )

    assert callback.size == expected_size
    assert callback.value == expected_size


def test_get_dir_callback(tmp_dir, dvc, cloud):
    cls, config, _ = get_cloud_fs(dvc, **cloud.config)
    fs = cls(**config)
    cloud.gen({"dir": {"foo": "foo", "bar": "bar"}})

    callback = Callback()
    fs.get(
        (cloud / "dir").fs_path, (tmp_dir / "dir").fs_path, callback=callback
    )

    assert callback.size == 2
    assert callback.value == 2
    assert (tmp_dir / "dir").read_text() == {"foo": "foo", "bar": "bar"}


def test_callback_on_dvcfs(tmp_dir, dvc, scm, mocker):
    tmp_dir.dvc_gen({"dir": {"bar": "bar"}}, commit="dvc")
    tmp_dir.scm_gen({"dir": {"foo": "foo"}}, commit="git")

    fs = dvc.dvcfs

    callback = Callback()
    fs.get(
        "dir",
        (tmp_dir / "dir2").fs_path,
        callback=callback,
    )

    assert (tmp_dir / "dir2").read_text() == {"foo": "foo", "bar": "bar"}
    assert callback.size == 2
    assert callback.value == 2

    callback = Callback()
    branch = mocker.spy(callback, "branch")
    fs.get(
        os.path.join("dir", "foo"),
        (tmp_dir / "foo").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "foo")
    assert (tmp_dir / "foo").read_text() == "foo"
    assert callback.size == 1
    assert callback.value == 1

    assert branch.call_count == 1
    assert branch.spy_return.size == size
    assert branch.spy_return.value == size

    branch.reset_mock()

    callback = Callback()
    branch = mocker.spy(callback, "branch")
    fs.get(
        os.path.join("dir", "bar"),
        (tmp_dir / "bar").fs_path,
        callback=callback,
    )

    size = os.path.getsize(tmp_dir / "dir" / "bar")
    assert (tmp_dir / "bar").read_text() == "bar"
    assert callback.size == 1
    assert callback.value == 1

    assert branch.call_count == 1
    assert branch.spy_return.size == size
    assert branch.spy_return.value == size


@pytest.mark.parametrize(
    "api", ["set_size", "relative_update", "absolute_update"]
)
@pytest.mark.parametrize(
    "callback_factory, kwargs",
    [
        (Callback.as_callback, {}),
        (Callback.as_tqdm_callback, {"desc": "test"}),
    ],
)
def test_callback_with_none(request, api, callback_factory, kwargs, mocker):
    """
    Test that callback don't fail if they receive None.

    The callbacks should not receive None, but there may be some
    filesystems that are not compliant, we may want to maintain
    maximum compatibility, and not break UI in these edge-cases.
    See https://github.com/iterative/dvc/issues/7704.
    """
    callback = callback_factory(**kwargs)
    request.addfinalizer(callback.close)

    call_mock = mocker.spy(callback, "call")
    method = getattr(callback, api)
    method(None)
    call_mock.assert_called_once_with()
    if callback is not DEFAULT_CALLBACK:
        assert callback.size is None
        assert callback.value == 0
