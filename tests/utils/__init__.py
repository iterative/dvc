import os
from contextlib import contextmanager
from filecmp import dircmp

from dvc.scm import Git


def get_gitignore_content():
    with open(Git.GITIGNORE, "r") as gitignore:
        return gitignore.read().splitlines()


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)


def trees_equal(dir_path_1, dir_path_2):

    comparison = dircmp(dir_path_1, dir_path_2)

    assert set(comparison.left_only) == set(comparison.right_only) == set()

    for d in comparison.common_dirs:
        trees_equal(os.path.join(dir_path_1, d), os.path.join(dir_path_2, d))


def to_posixpath(path):
    return path.replace("\\", "/")
