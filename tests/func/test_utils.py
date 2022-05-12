import re

import pytest

from dvc import utils
from dvc.exceptions import DvcException


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


def test_glob_no_match():
    with pytest.raises(
        DvcException, match=re.escape("Glob ['invalid*'] has no matches.")
    ):
        utils.glob_targets(["invalid*"], glob=True)
