"""
DVC
----
Make your data science projects reproducible and shareable.
"""
import warnings

import dvc.logger
from dvc.version import __version__  # noqa: F401


dvc.logger.setup()

# Ignore numpy's runtime warnings: https://github.com/numpy/numpy/pull/432.
# We don't directly import numpy, but our dependency networkx does, causing
# these warnings in some environments. Luckily these warnings are benign and
# we can simply ignore them so that they don't show up when you are using dvc.
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
