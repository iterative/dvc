import os

from dulwich.ignore import match_pattern, read_ignore_patterns
from dvc.utils.compat import cast_bytes
from dvc.utils.fs import get_parent_dirs_up_to


class DvcIgnoreFileHandler(object):
    def __init__(self, tree):
        self.tree = tree

    def read_patterns(self, path):
        with self.tree.open(path, binary=True) as stream:
            return self._read_patterns(stream)

    def get_repo_root(self):
        return self.tree.tree_root

    def _read_patterns(self, binary_stream):
        negate_patterns = []
        patterns = []
        for pattern in read_ignore_patterns(binary_stream):
            if pattern.lstrip().startswith(b"!"):
                negate_patterns.append(pattern)
            else:
                patterns.append(pattern)

        return negate_patterns, patterns


class DvcIgnore(object):
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnoreFromFile(DvcIgnore):
    def __init__(self, ignore_file_path, ignore_handler):
        self.ignore_file_path = ignore_file_path
        self.dirname = os.path.normpath(os.path.dirname(ignore_file_path))
        self.patterns = []
        self.negate_patterns = []

        self.negate_patterns, self.patterns = ignore_handler.read_patterns(
            ignore_file_path
        )

    def __call__(self, root, dirs, files):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d)]

        return dirs, files

    def get_match(self, abs_path):
        rel_path = os.path.relpath(abs_path, self.dirname)
        if os.name == "nt":
            rel_path = rel_path.replace("\\", "/")
        rel_path = cast_bytes(rel_path, "utf-8")

        for pattern in self.patterns:
            if match_pattern(
                rel_path, pattern
            ) and self._no_negate_pattern_matches(rel_path):
                return (abs_path, pattern, self.ignore_file_path)
        return None

    def matches(self, dirname, basename):
        if self.get_match(os.path.join(dirname, basename)):
            return True
        return False

    def _no_negate_pattern_matches(self, path):
        return all([not match_pattern(path, p) for p in self.negate_patterns])

    def __hash__(self):
        return hash(self.ignore_file_path)


class DvcIgnoreConstant(DvcIgnore):
    def __init__(self, basename):
        self.basename = basename


class DvcIgnoreDir(DvcIgnoreConstant):
    def __call__(self, root, dirs, files):
        dirs = [d for d in dirs if not d == self.basename]

        return dirs, files


class DvcIgnoreFile(DvcIgnoreConstant):
    def __call__(self, root, dirs, files):
        files = [f for f in files if not f == self.basename]

        return dirs, files


class DvcIgnoreFilter(object):
    def __init__(self, wdir, ignore_file_handler=None):
        self.ignores = [
            DvcIgnoreDir(".git"),
            DvcIgnoreDir(".hg"),
            DvcIgnoreDir(".dvc"),
            DvcIgnoreFile(".dvcignore"),
        ]

        self.ignore_file_handler = ignore_file_handler
        self._process_ignores_in_parent_dirs(wdir)

    def _process_ignores_in_parent_dirs(self, wdir):
        if self.ignore_file_handler:
            wdir = os.path.normpath(os.path.abspath(wdir))
            ignore_search_end_dir = self.ignore_file_handler.get_repo_root()
            parent_dirs = get_parent_dirs_up_to(wdir, ignore_search_end_dir)
            for d in parent_dirs:
                self.update(d)

    def update(self, wdir):
        ignore_file_path = os.path.join(wdir, DvcIgnore.DVCIGNORE_FILE)
        if os.path.exists(ignore_file_path):
            file_ignore = DvcIgnoreFromFile(
                ignore_file_path, ignore_handler=self.ignore_file_handler
            )
            self.ignores.append(file_ignore)

    def __call__(self, root, dirs, files):
        if self.ignore_file_handler:
            self.update(root)

        for ignore in self.ignores:
            dirs, files = ignore(root, dirs, files)

        return dirs, files
