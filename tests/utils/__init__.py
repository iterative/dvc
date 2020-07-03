import os
from contextlib import contextmanager

from dvc.scm import Git


def get_gitignore_content():
    with open(Git.GITIGNORE) as gitignore:
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
