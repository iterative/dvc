import pytest

from dvc.testing.scripts import _add_script


@pytest.fixture
def append_foo_script(tmp_dir):
    return _add_script(
        tmp_dir,
        "append_foo.py",
        """
import sys
from pathlib import Path

with Path(sys.argv[1]).open("a+", encoding="utf-8") as f:
    f.write("foo")
""".replace("\r\n", "\n"),
    )


@pytest.fixture
def head_script(tmp_dir):
    """Output first line of each file to different file with '-1' appended.
    Useful for tracking multiple outputs/dependencies which are not a copy
    of each others.
    """
    return _add_script(
        tmp_dir,
        "head.py",
        """
import sys
for file in sys.argv[1:]:
    with open(file) as f, open(file +"-1","w+") as w:
        w.write(f.readline())
""".replace("\r\n", "\n"),
    )
