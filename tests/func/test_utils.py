import pytest

from dvc import utils
from dvc.fs.local import LocalFileSystem


@pytest.mark.parametrize("dos2unix", [True, False])
def test_file_md5_crlf(tmp_dir, dos2unix):
    tmp_dir.gen("cr", b"a\nb\nc")
    tmp_dir.gen("crlf", b"a\r\nb\r\nc")
    fs = LocalFileSystem(None, {})
    eq = utils.file_md5("cr", fs, enable_d2u=dos2unix) == utils.file_md5(
        "crlf", fs, enable_d2u=dos2unix
    )
    assert eq == dos2unix


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
