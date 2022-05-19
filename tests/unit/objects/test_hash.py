from dvc_objects.fs import LocalFileSystem
from dvc_objects.hash import file_md5


def test_file_md5(tmp_path):
    foo = tmp_path / "foo"
    foo.write_text("foo content")

    fs = LocalFileSystem()
    assert file_md5(str(foo), fs) == file_md5(str(foo), fs)


def test_file_md5_crlf(tmp_path):
    fs = LocalFileSystem()
    cr = tmp_path / "cr"
    crlf = tmp_path / "crlf"
    cr.write_bytes(b"a\nb\nc")
    crlf.write_bytes(b"a\r\nb\r\nc")
    assert file_md5(str(cr), fs) == file_md5(str(crlf), fs)
