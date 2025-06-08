import os

import pytest

from dvc.utils import dict_sha256, fix_env, parse_target, relpath, resolve_output


@pytest.mark.skipif(os.name == "nt", reason="pyenv-win is not supported")
@pytest.mark.parametrize(
    "path, orig",
    [
        (
            "/pyenv/bin:/pyenv/libexec:/pyenv/plugins/plugin:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/pyenv/libexec:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/some/libexec:/pyenv/plugins/plugin:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        ("/orig/path1:/orig/path2", "/orig/path1:/orig/path2"),
        (
            "/orig/path1:/orig/path2:/pyenv/bin:/pyenv/libexec",
            "/orig/path1:/orig/path2:/pyenv/bin:/pyenv/libexec",
        ),
    ],
)
def test_fix_env_pyenv(path, orig):
    env = {
        "PATH": path,
        "PYENV_ROOT": "/pyenv",
        "PYENV_VERSION": "3.7.2",
        "PYENV_DIR": "/some/dir",
        "PYENV_HOOK_PATH": "/some/hook/path",
    }
    assert fix_env(env)["PATH"] == orig


@pytest.mark.skipif(os.name != "nt", reason="Windows specific")
def test_relpath_windows():
    assert relpath("x:\\dir1\\dir2\\file.txt", "x:\\dir1") == "dir2\\file.txt"

    assert (
        relpath("y:\\dir1\\dir2\\file.txt", "\\\\server\\share\\dir1")
        == "y:\\dir1\\dir2\\file.txt"
    )


@pytest.mark.parametrize(
    "inp,out,is_dir,expected",
    [
        ["target", None, False, "target"],
        ["target", "dir", True, os.path.join("dir", "target")],
        ["target", "file_target", False, "file_target"],
        [
            "target",
            os.path.join("dir", "subdir"),
            True,
            os.path.join("dir", "subdir", "target"),
        ],
        ["dir/", None, False, "dir"],
        ["dir", None, False, "dir"],
        ["dir", "other_dir", False, "other_dir"],
        ["dir", "other_dir", True, os.path.join("other_dir", "dir")],
    ],
)
def test_resolve_output(inp, out, is_dir, expected, mocker):
    mocker.patch("os.path.isdir", return_value=is_dir)
    result = resolve_output(inp, out)
    assert result == expected


@pytest.mark.parametrize(
    "inp,out, default",
    [
        ["dvc.yaml", ("dvc.yaml", None), None],
        ["dvc.yaml:name", ("dvc.yaml", "name"), None],
        [":name", ("dvc.yaml", "name"), None],
        ["stage.dvc", ("stage.dvc", None), None],
        ["../models/stage.dvc", ("../models/stage.dvc", None), "def"],
        [":name", ("default", "name"), "default"],
        ["something.dvc:name", ("something.dvc", "name"), None],
        ["../something.dvc:name", ("../something.dvc", "name"), None],
        ["file", (None, "file"), None],
        ["build@15", (None, "build@15"), None],
        ["build@{'level': 35}", (None, "build@{'level': 35}"), None],
        [":build@15", ("dvc.yaml", "build@15"), None],
        [":build@{'level': 35}", ("dvc.yaml", "build@{'level': 35}"), None],
        ["dvc.yaml:build@15", ("dvc.yaml", "build@15"), None],
        [
            "dvc.yaml:build@{'level': 35}",
            ("dvc.yaml", "build@{'level': 35}"),
            None,
        ],
        [
            "build2@{'level': [1, 2, 3]}",
            (None, "build2@{'level': [1, 2, 3]}"),
            None,
        ],
        [
            ":build2@{'level': [1, 2, 3]}",
            ("dvc.yaml", "build2@{'level': [1, 2, 3]}"),
            None,
        ],
        [
            "dvc.yaml:build2@{'level': [1, 2, 3]}",
            ("dvc.yaml", "build2@{'level': [1, 2, 3]}"),
            None,
        ],
    ],
)
def test_parse_target(inp, out, default):
    assert parse_target(inp, default) == out


def test_hint_on_lockfile():
    with pytest.raises(Exception, match="Did you mean: `dvc.yaml:name`?") as e:
        assert parse_target("dvc.lock:name")
    assert "dvc.yaml:name" in str(e.value)


@pytest.mark.parametrize(
    "d,sha",
    [
        (
            {
                "cmd": "echo content > out",
                "deps": {"dep": "2254342becceafbd04538e0a38696791"},
                "outs": {"out": "f75b8179e4bbe7e2b4a074dcef62de95"},
            },
            "f472eda60f09660a4750e8b3208cf90b3a3b24e5f42e0371d829710e9464d74a",
        ),
        (
            {
                "cmd": "echo content > out",
                "deps": {"dep": "2254342becceafbd04538e0a38696791"},
                "outs": ["out"],
            },
            "a239b67073bd58affcdb81fff3305d1726c6e7f9c86f3d4fca0e92e8147dc7b0",
        ),
    ],
)
def test_dict_sha256(d, sha):
    assert dict_sha256(d) == sha
