"""Handle import compatibility between Python 2 and Python 3"""

import sys
import os
import errno


# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3

# simplified version of ipython_genutils/encoding.py
DEFAULT_ENCODING = sys.getdefaultencoding()


def no_code(x, encoding=None):
    return x


def encode(u, encoding=None):
    encoding = encoding or DEFAULT_ENCODING
    return u.encode(encoding, "replace")


# NOTE: cast_bytes_py2 is taken from
# https://github.com/ipython/ipython_genutils
def cast_bytes(s, encoding=None):
    if not isinstance(s, bytes):
        return encode(s, encoding)
    return s


# NOTE _makedirs is taken from
# https://github.com/python/cpython/blob/
# 3ce3dea60646d8a5a1c952469a2eb65f937875b3/Lib/os.py#L196-L226
def _makedirs(name, mode=0o777, exist_ok=False):
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
        if isinstance(tail, bytes):
            cdir = bytes(os.curdir, "ASCII")
        if tail == cdir:
            return
    try:
        os.mkdir(name, mode)
    except OSError:
        if not exist_ok or not os.path.isdir(name):
            raise


if is_py2:
    from urlparse import urlparse, urljoin  # noqa: F401
    from StringIO import StringIO  # noqa: F401
    from BaseHTTPServer import HTTPServer  # noqa: F401
    from SimpleHTTPServer import SimpleHTTPRequestHandler  # noqa: F401
    import ConfigParser  # noqa: F401
    from io import open  # noqa: F401

    builtin_str = str  # noqa: F821
    bytes = str  # noqa: F821
    str = unicode  # noqa: F821
    basestring = basestring  # noqa: F821
    numeric_types = (int, long, float)  # noqa: F821
    integer_types = (int, long)  # noqa: F821
    input = raw_input  # noqa: F821
    cast_bytes_py2 = cast_bytes
    makedirs = _makedirs

elif is_py3:
    from os import makedirs  # noqa: F401
    from urllib.parse import urlparse, urljoin  # noqa: F401
    from io import StringIO  # noqa: F401
    from http.server import (  # noqa: F401
        HTTPServer,  # noqa: F401
        SimpleHTTPRequestHandler,  # noqa: F401
    )  # noqa: F401
    import configparser as ConfigParser  # noqa: F401

    builtin_str = str  # noqa: F821
    str = str  # noqa: F821
    bytes = bytes  # noqa: F821
    basestring = (str, bytes)  # noqa: F821
    numeric_types = (int, float)  # noqa: F821
    integer_types = (int,)  # noqa: F821
    input = input  # noqa: F821
    open = open  # noqa: F821
    cast_bytes_py2 = no_code
