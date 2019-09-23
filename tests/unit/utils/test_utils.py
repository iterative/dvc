import pytest

from dvc.utils import to_chunks, fix_env


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


@pytest.mark.parametrize(
    "path, orig",
    [
        (
            "/pyenv/bin:/pyenv/libexec:/pyenv/hook:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/pyenv/libexec:/orig/path1:/orig/path2",
            "/orig/path1:/orig/path2",
        ),
        (
            "/pyenv/bin:/some/libexec:/pyenv/hook:/orig/path1:/orig/path2",
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
    env = {"PATH": path, "PYENV_ROOT": "/pyenv"}
    assert fix_env(env)["PATH"] == orig
