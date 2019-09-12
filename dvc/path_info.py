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
    # Use __slots__ in PathInfo objects following PurePath implementation.
    # This makes objects smaller and speeds up attribute access.
    # We don't add any fields so it's empty.
    __slots__ = ()
    scheme = "local"

    def __new__(cls, *args):
        # Construct a proper subclass depending on current os
        if cls is PathInfo:
            cls = WindowsPathInfo if os.name == "nt" else PosixPathInfo
        return cls._from_parts(args)

    @classmethod
    def from_posix(cls, s):
        return cls(PosixPathInfo(s))

    def as_posix(self):
        f = self._flavour
        # Unlike original implementation [1] that uses `str()` we actually need
        # to use `fspath`, because we've overriden `__str__` method to return
        # relative paths, which will break original `as_posix`.
        #
        # [1] https://github.com/python/cpython/blob/v3.7.0/Lib/pathlib.py#L692
        return self.fspath.replace(f.sep, "/")

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
        # Use cached casefolded parts to compare paths
        n = len(other._cparts)
        return len(self._cparts) > n and self._cparts[:n] == other._cparts

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


class WindowsPathInfo(PathInfo, pathlib.PureWindowsPath):
    pass


class PosixPathInfo(PathInfo, pathlib.PurePosixPath):
    pass


class _URLPathParents(object):
    def __init__(self, pathcls, scheme, netloc, path):
        self._scheme = scheme
        self._netloc = netloc
        self._parents = path.parents
        self._pathcls = pathcls

    def __len__(self):
        return len(self._parents)

    def __getitem__(self, idx):
        return self._pathcls.from_parts(
            scheme=self._scheme,
            netloc=self._netloc,
            path=self._parents[idx].fspath,
        )

    def __repr__(self):
        return "<{}.parents>".format(self._pathcls.__name__)


class URLInfo(object):
    DEFAULT_PORTS = {"http": 80, "https": 443, "ssh": 22, "hdfs": 0}

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
        return "{}://{}{}".format(p.scheme, self.netloc, p.path)

    def __str__(self):
        return self.url

    def __repr__(self):
        return "{}: '{}'".format(type(self).__name__, self)

    def __eq__(self, other):
        if isinstance(other, basestring):
            other = self.__class__(other)
        return (
            self.__class__ == other.__class__
            and self.scheme == other.scheme
            and self.netloc == other.netloc
            and self._path == other._path
        )

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
        # When deepcopy is called, it creates and object without __init__,
        # self.parsed is not initialized and it causes infinite recursion.
        # More on this special casing here:
        # https://stackoverflow.com/a/47300262/298182
        if name.startswith("__"):
            raise AttributeError(name)
        return getattr(self.parsed, name)

    @cached_property
    def netloc(self):
        p = self.parsed
        netloc = p.hostname
        if p.username:
            netloc = p.username + "@" + netloc
        if p.port and int(p.port) != self.DEFAULT_PORTS.get(p.scheme):
            netloc += ":" + str(p.port)
        return netloc

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
        return PosixPathInfo(self.parsed.path)

    @property
    def name(self):
        return self._path.name

    @property
    def parts(self):
        return (self.scheme, self.netloc) + self._path.parts

    @property
    def bucket(self):
        return self.parsed.netloc

    @property
    def parent(self):
        return self.from_parts(
            scheme=self.scheme,
            netloc=self.parsed.netloc,
            path=self._path.parent.fspath,
        )

    @property
    def parents(self):
        return _URLPathParents(
            type(self), self.scheme, self.parsed.netloc, self._path
        )

    def relative_to(self, other):
        if isinstance(other, str):
            other = URLInfo(other)
        if self.scheme != other.scheme or self.netloc != other.netloc:
            raise ValueError(
                "'{}' does not start with '{}'".format(self, other)
            )
        return self._path.relative_to(other._path)

    def isin(self, other):
        if isinstance(other, basestring):
            other = self.__class__(other)
        elif self.__class__ != other.__class__:
            return False
        return (
            self.scheme == other.scheme
            and self.netloc == other.netloc
            and self._path.isin(other._path)
        )


class CloudURLInfo(URLInfo):
    @property
    def path(self):
        return self.parsed.path.lstrip("/")
