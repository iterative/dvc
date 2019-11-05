import errno
import os

from dvc.exceptions import DvcException
from dvc.scm.tree import BaseTree
from dvc.utils import relpath
from dvc.utils.compat import BytesIO
from dvc.utils.compat import StringIO


# see git-fast-import(1)
GIT_MODE_DIR = 0o40000
GIT_MODE_FILE = 0o644


def _iter_tree(tree):
    # During usual tree iteration with ``for .. in ..`` GitPython returns each
    # submodule instance without correctly filled ``name`` property. It raises
    # an exception during accessing such submodule ``name`` later.
    # The same time repo's submodules property contains correctly initialized
    # submodules list.
    # Here we just replace submodule instance from iterator with the
    # corresponding instance from ``repo.submodules.``
    submodules = {x.hexsha: x for x in tree.repo.submodules}
    for node in tree:
        if node.type == "submodule":
            node = submodules[node.hexsha]
        yield node


class GitTree(BaseTree):
    """Proxies the repo file access methods to Git objects"""

    def __init__(self, git, rev):
        """Create GitTree instance

        Args:
            git (dvc.scm.Git):
            branch:
        """
        self.git = git
        self.rev = rev

    @property
    def tree_root(self):
        return self.git.working_dir

    def open(self, path, mode="r", encoding="utf-8"):
        assert mode in {"r", "rb"}

        relative_path = relpath(path, self.git.working_dir)

        obj = self.git_object_by_path(path)
        if obj is None:
            msg = "No such file in branch '{}'".format(self.rev)
            raise IOError(errno.ENOENT, msg, relative_path)
        if obj.mode == GIT_MODE_DIR:
            raise IOError(errno.EISDIR, "Is a directory", relative_path)

        # GitPython's obj.data_stream is a fragile thing, it is better to
        # read it immediately, also it needs to be to decoded if we follow
        # the `open()` behavior (since data_stream.read() returns bytes,
        # and `open` with default "r" mode returns str)
        data = obj.data_stream.read()
        if mode == "rb":
            return BytesIO(data)
        return StringIO(data.decode(encoding))

    def exists(self, path):
        return self.git_object_by_path(path) is not None

    def isdir(self, path):
        obj = self.git_object_by_path(path)
        if obj is None:
            return False
        return obj.mode == GIT_MODE_DIR

    def isfile(self, path):
        obj = self.git_object_by_path(path)
        if obj is None:
            return False
        # according to git-fast-import(1) file mode could be 644 or 755
        return obj.mode & GIT_MODE_FILE == GIT_MODE_FILE

    @staticmethod
    def _is_tree_and_contains(obj, path):
        if obj.mode != GIT_MODE_DIR:
            return False
        # see https://github.com/gitpython-developers/GitPython/issues/851
        # `return (i in tree)` doesn't work so here is a workaround:
        for i in _iter_tree(obj):
            if i.name == path:
                return True
        return False

    def git_object_by_path(self, path):
        import git

        path = relpath(os.path.realpath(path), self.git.working_dir)
        assert path.split(os.sep, 1)[0] != ".."

        try:
            tree = self.git.tree(self.rev)
        except git.exc.BadName as exc:
            raise DvcException(
                "revision '{}' not found in git '{}'".format(
                    self.rev, os.path.relpath(self.git.working_dir)
                ),
                cause=exc,
            )

        if not path or path == ".":
            return tree
        for i in path.split(os.sep):
            if not self._is_tree_and_contains(tree, i):
                # there is no tree for specified path
                return None
            tree = tree[i]
        return tree

    def _walk(self, tree, topdown=True):
        dirs, nondirs = [], []
        for i in _iter_tree(tree):
            if i.mode == GIT_MODE_DIR:
                dirs.append(i.name)
            else:
                nondirs.append(i.name)

        if topdown:
            yield os.path.normpath(tree.abspath), dirs, nondirs

        for i in dirs:
            for x in self._walk(tree[i], topdown=True):
                yield x

        if not topdown:
            yield os.path.normpath(tree.abspath), dirs, nondirs

    def walk(self, top, topdown=True, dvcignore=None):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        - it could raise exceptions, there is no onerror argument
        """

        tree = self.git_object_by_path(top)
        if tree is None:
            raise IOError(errno.ENOENT, "No such file")

        for x in self._walk(tree, topdown):
            yield x
