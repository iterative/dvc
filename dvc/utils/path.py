import os
import posixpath

from dvc.path_info import HTTPURLInfo, URLInfo


def add_prefix(prefix, result, suffix):
    prefix = prefix or ""
    suffix = suffix or ""

    if isinstance(result, str):
        return prefix + result + suffix
    elif isinstance(result, tuple):
        return tuple(prefix + item + suffix for item in result)
    else:
        raise NotImplementedError(type(result).__name__)


def manipulation(
    func=None, /, remove_trailing=True, direct=False, abspath=False
):
    def manipulator(func):
        def wrapper(manager, maybe_path, *args, **kwargs):
            path = as_string(maybe_path)
            if abspath and manager.scheme == "local":
                path = os.path.abspath(path)

            if remove_trailing:
                path = manager._remove_trailing(path)

            if direct:
                real_path = path
                prefix, suffix = None, None
            else:
                prefix, real_path, suffix = manager.split_path(path)

            result = func(manager, path, *args, **kwargs)
            return add_prefix(prefix, result, suffix)

        return wrapper

    if func is None:
        return manipulator
    else:
        return manipulator(func)


class Proxy:
    def __init__(self, module, scheme):
        self.module = module
        self.sep = module.sep
        self.scheme = scheme

    def _remove_trailing(self, path: str) -> str:
        if not path.endswith(self.sep):
            return path

        new_path = path[: -len(self.sep)]
        assert len(new_path) > 0
        return new_path

    def split_path(self, path):
        from urllib.parse import urlparse, urlunparse

        if self.scheme == "local":
            return "", path, ""

        parse_result = urlparse(path)
        prefix = ""
        if parse_result.scheme:
            prefix += parse_result.scheme + "://"
        suffix = ""

        if set(parse_result.netloc) & {".", "@"} or parse_result.scheme in (
            "http",
            "https",
        ):
            prefix += parse_result.netloc
            suffix += urlunparse(
                parse_result._replace(netloc="", path="", scheme="")
            )
            real_path = parse_result.path
        else:
            real_path = "/" + parse_result.netloc.lstrip("/")
            if real_path.endswith("/"):
                real_path += parse_result.path.lstrip("/")
            else:
                real_path += parse_result.path

        return prefix, real_path, suffix

    def merge_path(self, prefix, real_path, suffix):
        path = ""
        if prefix:
            path += prefix
        path += real_path
        if suffix:
            path += suffix
        return path

    def join(self, *parts):
        parts = [as_string(part) for part in parts]

        if len(parts) == 0:
            return None
        elif len(parts) == 1:
            return parts[0]

        maybe_drive = parts[0]
        if not maybe_drive.endswith(self.sep):
            maybe_drive += self.sep

        real_parts = [maybe_drive]
        for index, part in enumerate(parts[1:], 1):
            if part.startswith(self.sep):
                part = part[len(self.sep) :]

            # For the last part, we are not going to adjust the end
            if index != len(parts) - 1 and not part.endswith(self.sep):
                part += self.sep

            real_parts.append(part)

        return "".join(real_parts)

    @manipulation
    def parts(self, path):
        prefix, real_path, suffix = self.split_path(path)

        real_parts = real_path.split(self.sep)

        # The root path should be stuff like /, C:\\
        # instead of '', C:.
        if manager.scheme == "local" and self.module.isabs(real_path):
            real_parts[0] += self.sep

        return tuple(real_parts)

    @manipulation(direct=False, abspath=True)
    def parent(self, path):
        parent, _ = path.rsplit(self.sep, 1)
        return parent

    @manipulation(abspath=True)
    def parents(self, path):
        prefix, real_path, suffix = self.split_path(path)

        parts = self.parts(real_path)
        return tuple(
            self.merge_path(prefix, self.join(*parts[:length]), suffix)
            for length in range(len(parts) - 1, 0, -1)
        )

    @manipulation
    def name(self, path):
        return self.parts(path)[-1]

    @manipulation
    def bucket(self, path):
        return self.parts(path)[1]

    @manipulation
    def url(self, path):
        # prefix://<path><?remainder>
        if self.scheme == "local":
            return path

        prefix, real_path, suffix = self.split_path(path)
        return self.merge_path(self.scheme + "://", real_path, suffix)

    @manipulation(direct=False)
    def suffix(self, path):
        name = self.name(path)
        _, _, suffix = name.partition(".")
        return suffix

    @manipulation(direct=False)
    def with_name(self, path, name):
        parts = list(self.parts(path))
        parts[-1] = name
        return self.join(*parts)

    @manipulation(direct=False)
    def with_suffix(self, path, suffix):
        parts = list(self.parts(path))
        real_path, dot, _ = parts[0].partition(".")
        parts[-1] = real_path + dot + suffix
        return self.join(*parts)

    def eq(self, left, right):
        return as_string(left) == as_string(right)

    def isin(self, left, right):
        left_parts = self.parts(left)
        right_parts = self.parts(right)
        left_len = len(left_parts)
        right_len = len(right_parts)
        return left_len > right_len and left_parts[:right_len] == right_parts

    def isin_or_eq(self, left, right):
        return self.eq(left, right) or self.isin(left, right)

    def overlaps(self, left, right):
        return self.isin_or_eq(left, right) or self.isin(right, left)

    def as_posix(self, path):
        return as_string(path).replace(self.module.sep, posixpath.sep)

    def isabs(self, path):
        return self.module.isabs(path)

    def abspath(self, path):
        return self.module.abspath(path)

    def relpath(self, *args):
        return self.module.relpath(*args)


manager = Proxy(os.path, "local")
url_manager = Proxy(posixpath, "url")


def as_string(path):
    if isinstance(path, URLInfo):
        # TODO: change this to a proper parser FQN
        if isinstance(path, HTTPURLInfo):
            return path.url
        elif "@" in path.bucket or "example.com" in path.bucket:
            return path.path
        else:
            return "/" + path.bucket.rstrip("/") + "/" + path.path.lstrip("/")
    elif path is None:
        return None

    res_path = os.fspath(path)
    if not isinstance(res_path, str):
        res_path = res_path.decode()

    return url_manager.split_path(res_path)[1]
