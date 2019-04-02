"""
DVC
----
Make your data science projects reproducible and shareable.
"""

from __future__ import unicode_literals

from dvc.version import __version__  # noqa: F401
import warnings
import dvc.logger


dvc.logger.setup()

# Ignore numpy's runtime warnings: https://github.com/numpy/numpy/pull/432.
# We don't directly import numpy, but our dependency networkx does, causing
# these warnings in some environments. Luckily these warnings are benign and
# we can simply ignore them so that they don't show up when you are using dvc.
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

# Ignore paramiko's warning: https://github.com/paramiko/paramiko/issues/1386.
# This only affects paramiko 2.4.2 and should be fixed in the next version.
# Cryptography developers decided that it is a brilliant idea not to inherit
# from DeprecationWarning [1] because it is invisible by default, and decided
# to spam everyone instead. So it is their fault and not paramiko's.
#
# [1] https://github.com/pyca/cryptography/blob/2.6.1/src/cryptography/
#     utils.py#L14
try:
    from cryptography.utils import CryptographyDeprecationWarning

    warnings.simplefilter("ignore", CryptographyDeprecationWarning)
except ImportError:
    pass
