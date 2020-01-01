"""Handle import compatibility between Python 2 and Python 3"""

import errno
import sys
from contextlib import contextmanager

# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3


@contextmanager
def ignore_file_not_found():
    try:
        yield
    except IOError as exc:
        if exc.errno != errno.ENOENT:
            raise


if is_py2:
    from urlparse import urlparse, urlunparse, urljoin  # noqa: F401
    from urllib import urlencode  # noqa: F401
    import ConfigParser  # noqa: F401
    from io import open  # noqa: F401
    import pathlib2 as pathlib  # noqa: F401
    from collections import Mapping  # noqa: F401
    from contextlib2 import ExitStack  # noqa: F401

    builtin_str = str  # noqa: F821
    bytes = str  # noqa: F821
    str = unicode  # noqa: F821
    basestring = basestring  # noqa: F821
    numeric_types = (int, long, float)  # noqa: F821
    integer_types = (int, long)  # noqa: F821
    input = raw_input  # noqa: F821
    range = xrange  # noqa: F821
    FileNotFoundError = IOError
    JSONDecodeError = ValueError

    import StringIO
    import io

    class StringIO(StringIO.StringIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    class BytesIO(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()


elif is_py3:
    import pathlib  # noqa: F401
    from urllib.parse import (  # noqa: F401
        urlparse,  # noqa: F401
        urlunparse,  # noqa: F401
        urlencode,  # noqa: F401
        urljoin,  # noqa: F401
    )
    from io import StringIO, BytesIO  # noqa: F401
    import configparser as ConfigParser  # noqa: F401
    from collections.abc import Mapping  # noqa: F401
    from contextlib import ExitStack  # noqa: F401
    from json.decoder import JSONDecodeError  # noqa: F401

    builtin_str = str  # noqa: F821
    str = str  # noqa: F821
    bytes = bytes  # noqa: F821
    basestring = (str, bytes)  # noqa: F821
    numeric_types = (int, float)  # noqa: F821
    integer_types = (int,)  # noqa: F821
    input = input  # noqa: F821
    open = open  # noqa: F821
    range = range  # noqa: F821
    FileNotFoundError = FileNotFoundError


# Backport os.fspath() from Python 3.6
try:
    from os import fspath  # noqa: F821

    fspath_py35 = lambda s: s  # noqa: E731
except ImportError:

    def fspath(path):
        """Return the path representation of a path-like object.

        If str or bytes is passed in, it is returned unchanged. Otherwise the
        os.PathLike interface is used to get the path representation. If the
        path representation is not str or bytes, TypeError is raised. If the
        provided path is not str, bytes, or os.PathLike, TypeError is raised.
        """
        if isinstance(path, (str, bytes)):
            return path

        # Work from the object's type to match method resolution of other magic
        # methods.
        path_type = type(path)
        try:
            path_repr = path_type.__fspath__(path)
        except AttributeError:
            if hasattr(path_type, "__fspath__"):
                raise
            else:
                raise TypeError(
                    "expected str, bytes or os.PathLike object, "
                    "not " + path_type.__name__
                )
        if isinstance(path_repr, (str, bytes)):
            return path_repr
        else:
            raise TypeError(
                "expected {}.__fspath__() to return str or bytes, "
                "not {}".format(path_type.__name__, type(path_repr).__name__)
            )

    fspath_py35 = fspath
