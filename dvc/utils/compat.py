"""Handle import compatibility between Python 2 and Python 3"""
from __future__ import absolute_import

import errno
import os
import sys
from contextlib import contextmanager

# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3

# simplified version of ipython_genutils/encoding.py
DEFAULT_ENCODING = sys.getdefaultencoding()

if _ver[:2] < (3, 5):
    RecursionError = RuntimeError
else:
    RecursionError = RecursionError


def no_code(x, encoding=None):
    return x


def encode(u, encoding=None):
    encoding = encoding or DEFAULT_ENCODING
    return u.encode(encoding, "replace")


def csv_reader(unicode_csv_data, dialect=None, **kwargs):
    """csv.reader doesn't support Unicode input, so need to use some tricks
    to work around this.

    Source: https://docs.python.org/2/library/csv.html#csv-examples
    """
    import csv

    dialect = dialect or csv.excel

    if is_py3:
        # Python3 supports encoding by default, so just return the object
        for row in csv.reader(unicode_csv_data, dialect=dialect, **kwargs):
            yield [cell for cell in row]

    else:
        # csv.py doesn't do Unicode; encode temporarily as UTF-8:
        reader = csv.reader(
            utf_8_encoder(unicode_csv_data), dialect=dialect, **kwargs
        )
        for row in reader:
            # decode UTF-8 back to Unicode, cell by cell:
            yield [unicode(cell, "utf-8") for cell in row]  # noqa: F821


def utf_8_encoder(unicode_csv_data):
    """Source: https://docs.python.org/2/library/csv.html#csv-examples"""
    for line in unicode_csv_data:
        yield line.encode("utf-8")


def cast_bytes(s, encoding=None):
    """Source: https://github.com/ipython/ipython_genutils"""
    if not isinstance(s, bytes):
        return encode(s, encoding)
    return s


def _makedirs(name, mode=0o777, exist_ok=False):
    """Source: https://github.com/python/cpython/blob/
        3ce3dea60646d8a5a1c952469a2eb65f937875b3/Lib/os.py#L196-L226
    """
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not os.path.exists(head):
        try:
            _makedirs(head, exist_ok=exist_ok)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        cdir = os.curdir
        if tail == cdir:
            return
    try:
        os.mkdir(name, mode)
    except OSError:
        if not exist_ok or not os.path.isdir(name):
            raise


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
    cast_bytes_py2 = cast_bytes
    makedirs = _makedirs
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

    def convert_to_unicode(data):
        if isinstance(data, builtin_str):
            return str(data)
        if isinstance(data, dict):
            return dict(map(convert_to_unicode, data.items()))
        if isinstance(data, (list, tuple)):
            return type(data)(map(convert_to_unicode, data))
        return data


elif is_py3:
    import pathlib  # noqa: F401
    from os import makedirs  # noqa: F401
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
    cast_bytes_py2 = no_code
    range = range  # noqa: F821
    FileNotFoundError = FileNotFoundError

    def convert_to_unicode(data):
        return data


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
