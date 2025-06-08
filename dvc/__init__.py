"""
DVC
----
Make your data science projects reproducible and shareable.
"""

import dvc.logger
from dvc.build import PKG  # noqa: F401
from dvc.version import __version__, version_tuple  # noqa: F401

dvc.logger.setup()
