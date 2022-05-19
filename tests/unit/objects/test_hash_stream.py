import pytest

from dvc_objects.fs import LocalFileSystem
from dvc_objects.hash import HashStreamFile, file_md5
from dvc_objects.istextfile import DEFAULT_CHUNK_SIZE, istextfile


def test_hashed_stream_reader(tmp_path):
    foo = tmp_path / "foo"
    foo.write_bytes(b"foo")

    with open(foo, "rb") as fobj:
        stream_reader = HashStreamFile(fobj)

        assert stream_reader.readable()
        assert not stream_reader.seekable()

        assert stream_reader.read(2) == b"fo"
        assert stream_reader.tell() == 2

        assert stream_reader.read(1) == b"o"
        assert stream_reader.tell() == 3

    hex_digest = file_md5(str(foo), LocalFileSystem())
    assert stream_reader.is_text
    assert hex_digest == stream_reader.hash_value


def test_hashed_stream_reader_as_chunks(tmp_path):
    foo = tmp_path / "foo"
    foo.write_bytes(b"foo \x00" * 16)

    actual_size = len(foo.read_bytes())
    with open(foo, "rb") as fobj:
        stream_reader = HashStreamFile(fobj)

        total_read = 0
        while True:
            chunk = stream_reader.read(16)
            total_read += len(chunk)
            assert stream_reader.tell() == total_read
            if not chunk:
                break

        assert stream_reader.tell() == actual_size == total_read

    hex_digest = file_md5(str(foo), LocalFileSystem())
    assert not stream_reader.is_text
    assert hex_digest == stream_reader.hash_value


@pytest.mark.parametrize(
    "contents",
    [b"x" * DEFAULT_CHUNK_SIZE + b"\x00", b"clean", b"not clean \x00"],
)
def test_hashed_stream_reader_compatibility(tmp_path, contents):
    # Always read more than the DEFAULT_CHUNK_SIZE (512 bytes).
    # This imitates the read actions performed by upload_fobj.
    chunk_size = DEFAULT_CHUNK_SIZE * 2

    data = tmp_path / "data"
    data.write_bytes(contents)

    with open(data, "rb") as fobj:
        stream_reader = HashStreamFile(fobj)
        stream_reader.read(chunk_size)

    local_fs = LocalFileSystem()
    hex_digest = file_md5(str(data), local_fs)

    assert stream_reader.is_text is istextfile(str(data), local_fs)
    assert stream_reader.hash_value == hex_digest
