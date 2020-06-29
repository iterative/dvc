import errno
import io
import os
import stat

from dvc.exceptions import DvcException
from dvc.scm.tree import BaseTree
from dvc.utils import relpath

# see git-fast-import(1)
GIT_MODE_DIR = 0o40000
GIT_MODE_FILE = 0o644


def _item_basename(item):
    # NOTE: `item.name` is not always a basename. See [1] for more details.
    #
    # [1] https://github.com/iterative/dvc/issues/3481#issuecomment-600693884
    return os.path.basename(item.path)


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

        obj = self._git_object_by_path(path)
        if obj is None:
            msg = f"No such file in branch '{self.rev}'"
            raise OSError(errno.ENOENT, msg, relative_path)
        if obj.mode == GIT_MODE_DIR:
            raise OSError(errno.EISDIR, "Is a directory", relative_path)

        # GitPython's obj.data_stream is a fragile thing, it is better to
        # read it immediately, also it needs to be to decoded if we follow
        # the `open()` behavior (since data_stream.read() returns bytes,
        # and `open` with default "r" mode returns str)
        data = obj.data_stream.read()
        if mode == "rb":
            return io.BytesIO(data)
        return io.StringIO(data.decode(encoding))

    def exists(self, path):
        return self._git_object_by_path(path) is not None

    def isdir(self, path):
        obj = self._git_object_by_path(path)
        if obj is None:
            return False
        return obj.mode == GIT_MODE_DIR

    def isfile(self, path):
        obj = self._git_object_by_path(path)
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
        for item in obj:
            if _item_basename(item) == path:
                return True
        return False

    def _git_object_by_path(self, path):
        import git

        path = relpath(os.path.realpath(path), self.git.working_dir)
        if path.split(os.sep, 1)[0] == "..":
            # path points outside of git repository
            return None

        try:
            tree = self.git.tree(self.rev)
        except git.exc.BadName as exc:  # pylint: disable=no-member
            raise DvcException(
                "revision '{}' not found in Git '{}'".format(
                    self.rev, os.path.relpath(self.git.working_dir)
                )
            ) from exc

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
        for item in tree:
            name = _item_basename(item)
            if item.mode == GIT_MODE_DIR:
                dirs.append(name)
            else:
                nondirs.append(name)

        if topdown:
            yield os.path.normpath(tree.abspath), dirs, nondirs

        for i in dirs:
            yield from self._walk(tree[i], topdown=topdown)

        if not topdown:
            yield os.path.normpath(tree.abspath), dirs, nondirs

    def walk(self, top, topdown=True, onerror=None):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """

        tree = self._git_object_by_path(top)
        if tree is None:
            if onerror is not None:
                onerror(OSError(errno.ENOENT, "No such file", top))
            return
        if tree.mode != GIT_MODE_DIR:
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        yield from self._walk(tree, topdown=topdown)

    def isexec(self, path):
        if not self.exists(path):
            return False

        mode = self.stat(path).st_mode
        return mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def stat(self, path):
        import git

        def to_ctime(git_time):
            sec, nano_sec = git_time
            return sec + nano_sec / 1000000000

        obj = self._git_object_by_path(path)
        if obj is None:
            raise OSError(errno.ENOENT, "No such file")
        entry = git.index.IndexEntry.from_blob(obj)

        # os.stat_result takes a tuple in the form:
        #   (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
        return os.stat_result(
            (
                entry.mode,
                entry.inode,
                entry.dev,
                0,
                entry.uid,
                entry.gid,
                entry.size,
                # git index has no atime equivalent, use mtime
                to_ctime(entry.mtime),
                to_ctime(entry.mtime),
                to_ctime(entry.ctime),
            )
        )

    @property
    def hash_jobs(self):
        # NOTE: gitpython is not threadsafe. See
        # https://github.com/iterative/dvc/issues/4079
        return 1
