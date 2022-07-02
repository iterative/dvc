"""
DVC
----
Make your data science projects reproducible and shareable.
"""
import dvc.logger

from .version import __version__  # noqa: F401

dvc.logger.setup()
