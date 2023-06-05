import pytest

COPY_SCRIPT = """
import os
import shutil
import sys

if os.path.isfile(sys.argv[1]):
    shutil.copyfile(sys.argv[1], sys.argv[2])
else:
    shutil.copytree(sys.argv[1], sys.argv[2])
""".replace(
    "\r\n", "\n"
)


def _add_script(tmp_dir, path, contents=""):
    script, *_ = tmp_dir.gen(path, contents.encode("utf-8"))
    if hasattr(tmp_dir, "scm"):
        tmp_dir.scm_add(path, commit=f"add {path}")
    return script.fs_path


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
""".replace(
            "\r\n", "\n"
        ),
    )


@pytest.fixture
def copy_script(
    tmp_dir,
):
    return _add_script(tmp_dir, "copy.py", COPY_SCRIPT)


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
""".replace(
            "\r\n", "\n"
        ),
    )
