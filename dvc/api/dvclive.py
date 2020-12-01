import os
from typing import List

from dvc.exceptions import NotDvcRepoError
from dvc.repo import Repo


def summary(path: str, revs: List[str] = None):
    try:
        root = Repo.find_root()
    except NotDvcRepoError:
        root = os.getcwd()

    Repo(root_dir=root, uninitialized=True).dvclive.summarize(path, revs)
