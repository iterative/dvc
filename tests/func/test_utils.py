# encoding: utf-8
import os
import stat

import pytest

from dvc import utils


def test_copyfile(tmp_dir):
    src = "file1"
    dest = "file2"
    dest_dir = "testdir"

    tmp_dir.gen(src, "file1contents")

    os.mkdir(dest_dir)

    utils.copyfile(src, dest)
    assert (tmp_dir / dest).read_text() == "file1contents"

    utils.copyfile(src, dest_dir)
    assert (tmp_dir / dest_dir / src).read_text() == "file1contents"


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


@pytest.mark.skipif(os.name == "nt", reason="Not supported for Windows.")
def test_makedirs_permissions(tmp_dir):
    dir_mode = 0o755
    intermediate_dir = "тестовая-директория"
    test_dir = os.path.join(intermediate_dir, "data")

    assert not os.path.exists(intermediate_dir)

    utils.makedirs(test_dir, mode=dir_mode)

    assert stat.S_IMODE(os.stat(test_dir).st_mode) == dir_mode
    assert stat.S_IMODE(os.stat(intermediate_dir).st_mode) == dir_mode
