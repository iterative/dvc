from pathlib import Path

import pytest

from dvc import utils
from dvc.utils import resolve_paths


def test_file_md5_crlf(tmp_dir):
    tmp_dir.gen("cr", b"a\nb\nc")
    tmp_dir.gen("crlf", b"a\r\nb\r\nc")
    assert utils.file_md5("cr")[0] == utils.file_md5("crlf")[0]


def test_dict_md5():
    d = {
        "cmd": "python code.py foo file1",
        "locked": "true",
        "outs": [
            {
                "path": "file1",
                "metric": {"type": "raw"},
                "cache": False,
                "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
            }
        ],
        "deps": [
            {"path": "foo", "md5": "acbd18db4cc2f85cedef654fccc4a4d8"},
            {"path": "code.py", "md5": "d05447644b89960913c7eee5fd776adb"},
        ],
    }

    md5 = "8b263fa05ede6c3145c164829be694b4"

    assert md5 == utils.dict_md5(d, exclude=["metric", "locked"])


def test_boxify():
    expected = (
        "+-----------------+\n"
        "|                 |\n"
        "|     message     |\n"
        "|                 |\n"
        "+-----------------+\n"
    )

    assert expected == utils.boxify("message")


@pytest.mark.parametrize(
    "filename", [None, "file"],
)
def test_resolve_paths_simple_filename(dvc, filename):
    wdir, out = resolve_paths(dvc, filename)

    assert dvc.root_dir == wdir
    assert out == filename


@pytest.mark.parametrize(
    "path", ["dir", "dir/with/sub", "./another/dir"],
)
def test_resolve_paths_filename_with_path(dvc, path):
    filename = "file"
    out = str(Path(path).joinpath(filename))
    wdir, out = resolve_paths(dvc, out)

    assert Path(dvc.root_dir).joinpath(path) == Path(wdir)
    assert out == filename


def test_resolve_paths_dot(dvc):
    wdir, out = resolve_paths(dvc, ".")

    assert dvc.root_dir == wdir
    assert out is None


@pytest.mark.parametrize(
    "path", ["", ".", "sub/with", "./another"],
)
@pytest.mark.parametrize(
    "force_out", [False, True],
)
def test_resolve_paths_dirs(dvc, path, force_out):
    last_dir = "dir"
    partial_path = Path(path).joinpath(last_dir)
    target_path = Path(dvc.root_dir).joinpath(partial_path)
    target_path.mkdir(parents=True)

    wdir, out = resolve_paths(dvc, str(partial_path), force_out=force_out)

    if force_out:
        assert target_path.parent == Path(wdir)
        assert out == last_dir
    else:
        assert target_path == Path(wdir)
        assert out is None


def test_resolve_paths_out_of_repo(dvc):
    filename = "/path/out/of/the/repo"
    wdir, out = resolve_paths(dvc, filename)

    assert dvc.root_dir == wdir
    assert out == filename
