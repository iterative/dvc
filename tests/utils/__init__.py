import csv
import os
from contextlib import contextmanager

import pytest
from funcy import first

from dvc.scm import Git

# rewrite assertions in assert, pytest does not rewrite for other modules
# than tests itself.
pytest.register_assert_rewrite("tests.utils.asserts")


def get_gitignore_content():
    with open(Git.GITIGNORE, encoding="utf-8") as gitignore:
        return gitignore.read().splitlines()


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


def to_posixpath(path):
    return path.replace("\\", "/")


def dump_sv(stream, metrics, delimiter=",", header=True):
    if header:
        writer = csv.DictWriter(
            stream, fieldnames=list(first(metrics).keys()), delimiter=delimiter
        )
        writer.writeheader()
        writer.writerows(metrics)
    else:
        writer = csv.writer(stream)
        for d in metrics:
            writer.writerow(list(d.values()))


def clean_staging():
    from dvc.fs.memory import MemoryFileSystem
    from dvc.objects.stage import _STAGING_MEMFS_PATH

    try:
        MemoryFileSystem().fs.rm(
            f"memory://{_STAGING_MEMFS_PATH}", recursive=True
        )
    except FileNotFoundError:
        pass
