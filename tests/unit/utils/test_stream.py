from dvc.utils import file_md5
from dvc.utils.stream import HashedStreamReader


def test_hashed_stream_reader(tmp_dir):
    tmp_dir.gen({"foo": "foo"})

    foo = tmp_dir / "foo"
    with open(foo, "rb") as fobj:
        stream_reader = HashedStreamReader(fobj)
        stream_reader.read()

    hex_digest, _ = file_md5(foo)
    assert stream_reader.is_text_file
    assert hex_digest == stream_reader.hash_info.value


def test_hashed_stream_reader_as_chunks(tmp_dir):
    tmp_dir.gen({"foo": b"foo \x00" * 16})

    foo = tmp_dir / "foo"
    with open(foo, "rb") as fobj:
        stream_reader = HashedStreamReader(fobj)
        while True:
            chunk = stream_reader.read(16)
            if not chunk:
                break

    hex_digest, _ = file_md5(foo)
    assert not stream_reader.is_text_file
    assert hex_digest == stream_reader.hash_info.value
