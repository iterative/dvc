from dvc.fs import LocalFileSystem
from dvc.objects.hash import file_md5


def test_file_md5(tmp_dir):
    tmp_dir.gen("foo", "foo content")

    fs = LocalFileSystem()
    assert file_md5("foo", fs) == file_md5("foo", fs)


def test_file_md5_crlf(tmp_dir):
    fs = LocalFileSystem()
    tmp_dir.gen("cr", b"a\nb\nc")
    tmp_dir.gen("crlf", b"a\r\nb\r\nc")
    assert file_md5("cr", fs) == file_md5("crlf", fs)
