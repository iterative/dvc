import os
import pathlib
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


def load_gdrive_credentials(root_dir):
    current_dir = pathlib.Path(__file__).parent.parent
    gdrive_credentials = current_dir.joinpath("gdrive-user-credentials.json")

    if gdrive_credentials.exists():
        import shutil

        inner_tmp = pathlib.Path(root_dir).joinpath(".dvc", "tmp")
        inner_tmp.mkdir(exist_ok=True)
        shutil.copy(gdrive_credentials, inner_tmp)
        return True

    return False
