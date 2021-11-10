import posixpath


class Path:
    """
    Class for operations on simple string paths.

    This is meant to be very efficient and so doesn't use os.path,
    doesn't have any notion of cwd and assumes that we are always
    operating on absolute paths.
    """

    def __init__(self, sep: str):
        self.sep = sep

    def join(self, *parts):
        if len(parts) == 0:
            return None
        if len(parts) == 1:
            return parts[0]

        return self.sep.join(parts)

    def parts(self, path):
        return tuple(path.split(self.sep))

    def parent(self, path):
        parts = path.rsplit(self.sep, 1)
        if len(parts) == 1:
            return ""
        return parts[0]

    def parents(self, path):
        parts = self.parts(path)
        return tuple(
            self.join(*parts[:length])
            for length in range(len(parts) - 1, 0, -1)
        )

    def name(self, path):
        return self.parts(path)[-1]

    def suffix(self, path):
        name = self.name(path)
        _, dot, suffix = name.partition(".")
        return dot + suffix

    def with_name(self, path, name):
        parts = list(self.parts(path))
        parts[-1] = name
        return self.join(*parts)

    def with_suffix(self, path, suffix):
        parts = list(self.parts(path))
        real_path, _, _ = parts[-1].partition(".")
        parts[-1] = real_path + suffix
        return self.join(*parts)

    def isin(self, left, right):
        left_parts = self.parts(left)
        right_parts = self.parts(right)
        left_len = len(left_parts)
        right_len = len(right_parts)
        return left_len > right_len and left_parts[:right_len] == right_parts

    def isin_or_eq(self, left, right):
        return left == right or self.isin(left, right)

    def overlaps(self, left, right):
        # pylint: disable=arguments-out-of-order
        return self.isin_or_eq(left, right) or self.isin(right, left)

    def relpath(self, path, base):
        assert len(path) > len(base)
        assert path.startswith(base)
        normpath = path.rstrip(self.sep)
        normbase = base.rstrip(self.sep)
        return normpath[len(normbase) + 1 :]

    def relparts(self, path, base):
        return self.parts(self.relpath(path, base))

    def as_posix(self, path):
        return path.replace(self.sep, posixpath.sep)
