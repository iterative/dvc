"""Handle import compatibility between Python 2 and Python 3"""

import sys


# Syntax sugar.
_ver = sys.version_info

#: Python 2.x?
is_py2 = _ver[0] == 2

#: Python 3.x?
is_py3 = _ver[0] == 3

# NOTE: cast_bytes_py2 is taken from https://github.com/ipython/ipython_genutils

# simplified version of ipython_genutils/encoding.py
DEFAULT_ENCODING = sys.getdefaultencoding()


def no_code(x, encoding=None):
    return x


def encode(u, encoding=None):
    encoding = encoding or DEFAULT_ENCODING
    return u.encode(encoding, "replace")


def cast_bytes(s, encoding=None):
    if not isinstance(s, bytes):
        return encode(s, encoding)
    return s


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

elif is_py3:
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
