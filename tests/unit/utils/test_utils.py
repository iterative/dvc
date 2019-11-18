import filecmp
import re
import os

import pytest

from dvc.path_info import PathInfo
from dvc.utils import copyfile
from dvc.utils import file_md5
from dvc.utils import fix_env
from dvc.utils import makedirs
from dvc.utils import to_chunks
from dvc.utils import tmp_fname
from tests.basic_env import TestDir


@pytest.mark.parametrize(
    "chunk_size, expected_chunks",
    [(1, [[1], [2], [3], [4]]), (2, [[1, 2], [3, 4]]), (3, [[1, 2, 3], [4]])],
)
def test_to_chunks_chunk_size(chunk_size, expected_chunks):
    list_to_chunk = [1, 2, 3, 4]
    result = list(to_chunks(list_to_chunk, chunk_size=chunk_size))
    assert result == expected_chunks


@pytest.mark.parametrize("num_chunks, chunk_size", [(1, 2), (None, None)])
def test_to_chunks_should_raise(num_chunks, chunk_size):
    list_to_chunk = [1, 2, 3]
    with pytest.raises(ValueError):
        to_chunks(list_to_chunk, num_chunks, chunk_size)


@pytest.mark.parametrize(
    "num_chunks, expected_chunks",
    [(4, [[1], [2], [3], [4]]), (3, [[1, 2], [3, 4]]), (2, [[1, 2], [3, 4]])],
)
def test_to_chunks_num_chunks(num_chunks, expected_chunks):
    list_to_chunk = [1, 2, 3, 4]
    result = to_chunks(list_to_chunk, num_chunks=num_chunks)
    assert result == expected_chunks


@pytest.mark.skipif(os.name == "nt", reason="pyenv-win is not supported")
@pytest.mark.parametrize(
    "path, orig",
    [
        (
            (
                "/pyenv/bin:/pyenv/libexec:/pyenv/plugins/plugin:"
                "/orig/path1:/orig/path2"
            ),
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/pyenv/libexec:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            (
                "/pyenv/bin:/some/libexec:/pyenv/plugins/plugin:"
                "/orig/path1:/orig/path2"
            ),
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


def test_file_md5(repo_dir):
    fname = repo_dir.FOO
    fname_object = PathInfo(fname)
    assert file_md5(fname) == file_md5(fname_object)


@pytest.mark.parametrize("path", [TestDir.DATA, TestDir.DATA_DIR])
def test_copyfile(path, repo_dir):
    src = repo_dir.FOO
    dest = path
    src_info = PathInfo(repo_dir.BAR)
    dest_info = PathInfo(path)

    copyfile(src, dest)
    if os.path.isdir(dest):
        assert filecmp.cmp(
            src, os.path.join(dest, os.path.basename(src)), shallow=False
        )
    else:
        assert filecmp.cmp(src, dest, shallow=False)

    copyfile(src_info, dest_info)
    if os.path.isdir(dest_info.fspath):
        assert filecmp.cmp(
            src_info.fspath,
            os.path.join(dest_info.fspath, os.path.basename(src_info.fspath)),
            shallow=False,
        )
    else:
        assert filecmp.cmp(src_info.fspath, dest_info.fspath, shallow=False)


def test_makedirs(repo_dir):
    path = os.path.join(repo_dir.root_dir, "directory")
    path_info = PathInfo(
        os.path.join(repo_dir.root_dir, "another", "directory")
    )

    makedirs(path)
    assert os.path.isdir(path)

    makedirs(path_info)
    assert os.path.isdir(path_info.fspath)


def test_tmp_fname():
    file_path = os.path.join("path", "to", "file")
    file_path_info = PathInfo(file_path)

    def pattern(path):
        return r"^" + re.escape(path) + r"\.[a-z0-9]{22}\.tmp$"

    assert re.search(pattern(file_path), tmp_fname(file_path), re.IGNORECASE)
    assert re.search(
        pattern(file_path_info.fspath),
        tmp_fname(file_path_info),
        re.IGNORECASE,
    )
