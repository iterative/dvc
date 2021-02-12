"""Draws DAG in ASCII."""

import logging
import os
import pydoc
import sys

from rich.pager import Pager

from dvc.env import DVC_PAGER
from dvc.utils import format_link

logger = logging.getLogger(__name__)


DEFAULT_PAGER = "less"
DEFAULT_PAGER_FORMATTED = (
    f"{DEFAULT_PAGER} --chop-long-lines --clear-screen --RAW-CONTROL-CHARS"
)


def make_pager(cmd):
    def _pager(text):
        return pydoc.tempfilepager(pydoc.plain(text), cmd)

    return _pager


def find_pager():
    if not sys.stdout.isatty():
        return pydoc.plainpager

    env_pager = os.getenv(DVC_PAGER)
    if env_pager:
        return make_pager(env_pager)

    if os.system(f"({DEFAULT_PAGER}) 2>{os.devnull}") == 0:
        return make_pager(DEFAULT_PAGER_FORMATTED)

    logger.warning(
        "Unable to find `less` in the PATH. Check out "
        "{} for more info.".format(
            format_link("https://man.dvc.org/pipeline/show")
        )
    )
    return pydoc.plainpager


def pager(text):
    find_pager()(text)


class DvcPager(Pager):
    def show(self, content: str) -> None:
        pager(content)
