"""
DVC
----
Make your data science projects reproducible and shareable.
"""
from typing import Optional

import dvc.logger
from dvc import _build
from dvc.version import __version__, version_tuple  # noqa: F401

PKG: "Optional[str]" = _build.PKG
dvc.logger.setup()
