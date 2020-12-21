import logging
import os
from typing import List

from dvc.exceptions import NotDvcRepoError
from dvc.repo import Repo
from dvc.utils.html import write

logger = logging.getLogger(__name__)


def summary(path: str, revs: List[str] = None):
    try:
        root = Repo.find_root()
    except NotDvcRepoError:
        root = os.getcwd()

    metrics, plots = Repo(root_dir=root, uninitialized=True).live.show(
        path, revs
    )

    html_path = path + ".html"
    write(html_path, plots, metrics)
    logger.info(f"\nfile://{os.path.abspath(html_path)}")
