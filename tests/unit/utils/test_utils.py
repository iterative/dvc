import pytest

from dvc.utils import chunk


@pytest.mark.parametrize(
    "list_to_chunk, chunk_size, expected_chunks",
    [
        ([1, 2, 3, 4], 1, [[1], [2], [3], [4]]),
        ([1, 2, 3, 4], 2, [[1, 2], [3, 4]]),
        ([1, 2, 3, 4], 3, [[1, 2, 3], [4]]),
    ],
)
def test_chunk(list_to_chunk, chunk_size, expected_chunks):
    result = list(chunk(list_to_chunk, chunk_size))
    assert result == expected_chunks
