from __future__ import unicode_literals, absolute_import

import sys
import os
import posixpath
from funcy import cached_property

from dvc.utils.compat import str, builtin_str, basestring, is_py2
from dvc.utils.compat import pathlib, urlparse


# On Python 2.7/Windows sys.getfilesystemencoding() is set to mbcs,
# which is lossy, thus we can't use that,
# see https://github.com/mcmtroffaes/pathlib2/issues/56.
from dvc.utils import relpath

if is_py2:
    fs_encoding = "utf-8"


class PathInfo(pathlib.PurePath):
    scheme = "local"

    def __new__(cls, *args):
        # Construct a proper subclass depending on current os
        if cls is PathInfo:
            cls = WindowsPathInfo if os.name == "nt" else PosixPathInfo
        return cls._from_parts(args)

    @classmethod
    def from_posix(cls, s):
        return cls(PosixPathInfo(s))

    def __str__(self):
        path = self.__fspath__()
        return relpath(path)

    def __repr__(self):
        return builtin_str("{}: '{}'").format(type(self).__name__, self)

    # This permits passing it to file utils directly in Python 3.6+
    # With Python 2.7, Python 3.5+ we are stuck with path_info.fspath for now
    def __fspath__(self):
        return pathlib.PurePath.__str__(self)

    @property
    def fspath(self):
        return self.__fspath__()

    url = fspath

    def relpath(self, other):
        return self.__class__(relpath(self, other))

    def isin(self, other):
        if isinstance(other, basestring):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        return any(p == other for p in self.parents)

    # pathlib2 uses bytes internally in Python 2, and we use unicode everywhere
    # for paths in both pythons, thus we need this glue.
    if is_py2:
        __unicode__ = __str__

        def __str__(self):
            return self.__unicode__().encode(sys.getfilesystemencoding())

        @classmethod
        def _parse_args(cls, args):
            args = [
                a.encode(fs_encoding)
                if isinstance(a, unicode)  # noqa: F821
                else a
                for a in args
            ]
            return super(PathInfo, cls)._parse_args(args)

        @property
        def name(self):
            return super(PathInfo, self).name.decode(fs_encoding)

        def __fspath__(self):  # noqa: F811
            return pathlib.PurePath.__str__(self).decode(fs_encoding)

        def with_name(self, name):
            return pathlib.PurePath.with_name(self, name.encode(fs_encoding))

        def as_posix(self):
            f = self._flavour
            return str(self).replace(f.sep, "/")


class WindowsPathInfo(PathInfo, pathlib.PureWindowsPath):
    pass


class PosixPathInfo(PathInfo, pathlib.PurePosixPath):
    pass


class URLInfo(object):
    DEFAULT_PORTS = {"http": 80, "https": 443, "ssh": 22}

    def __init__(self, url):
        self.parsed = urlparse(url)
        assert self.parsed.scheme != "remote"

    @classmethod
    def from_parts(
        cls, scheme=None, netloc=None, host=None, user=None, port=None, path=""
    ):
        assert scheme and (bool(host) ^ bool(netloc))

        if netloc is None:
            netloc = host
            if user:
                netloc = user + "@" + host
            if port:
                netloc += ":" + str(port)
        return cls("{}://{}{}".format(scheme, netloc, path))

    @cached_property
    def url(self):
        p = self.parsed
        netloc = p.hostname
        if p.username:
            netloc = p.username + "@" + netloc
        if p.port and int(p.port) != self.DEFAULT_PORTS.get(p.scheme):
            netloc += ":" + str(p.port)
        return "{}://{}{}".format(p.scheme, netloc, p.path)

    def __str__(self):
        return self.url

    def __repr__(self):
        return "{}: '{}'".format(type(self).__name__, self)

    def __eq__(self, other):
        if isinstance(other, basestring):
            other = self.__class__(other)
        return str(self) == str(other)

    def __hash__(self):
        return hash(self.url)

    def __div__(self, other):
        p = self.parsed
        new_path = posixpath.join(p.path, str(other))
        if not new_path.startswith("/"):
            new_path = "/" + new_path
        new_url = "{}://{}{}".format(p.scheme, p.netloc, new_path)
        return self.__class__(new_url)

    __truediv__ = __div__

    def __getattr__(self, name):
        return getattr(self.parsed, name)

    @property
    def port(self):
        return self.parsed.port or self.DEFAULT_PORTS.get(self.parsed.scheme)

    @property
    def host(self):
        return self.parsed.hostname

    @property
    def user(self):
        return self.parsed.username

    @cached_property
    def _path(self):
        return pathlib.PurePosixPath(self.parsed.path)

    @property
    def name(self):
        return self._path.name

    @property
    def parts(self):
        return (self.scheme, self.netloc) + self._path.parts

    @property
    def bucket(self):
        return self.netloc

    @property
    def parent(self):
        return self.from_parts(
            scheme=self.scheme, netloc=self.netloc, path=self._path.parent
        )

    def isin(self, other):
        if isinstance(other, basestring):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        return (
            self.scheme == other.scheme
            and self.netloc == other.netloc
            and PathInfo(self.path).isin(PathInfo(other.path))
        )


class CloudURLInfo(URLInfo):
    @property
    def path(self):
        return self.parsed.path.lstrip("/")
