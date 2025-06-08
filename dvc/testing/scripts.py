import pytest

COPY_SCRIPT = """
import os
import shutil
import sys

if os.path.isfile(sys.argv[1]):
    shutil.copyfile(sys.argv[1], sys.argv[2])
else:
    shutil.copytree(sys.argv[1], sys.argv[2])
""".replace("\r\n", "\n")


def _add_script(tmp_dir, path, contents=""):
    script, *_ = tmp_dir.gen(path, contents.encode("utf-8"))
    if hasattr(tmp_dir, "scm"):
        tmp_dir.scm_add(path, commit=f"add {path}")
    return script.fs_path


@pytest.fixture
def copy_script(tmp_dir):
    return _add_script(tmp_dir, "copy.py", COPY_SCRIPT)
