import logging
import os
import re
from collections import namedtuple
from itertools import chain, groupby, takewhile

from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file
from pygtrie import Trie

from dvc.fs import AnyFSPath, FileSystem, Schemes, localfs
from dvc.pathspec_math import PatternInfo, merge_patterns
from dvc.types import List, Optional

logger = logging.getLogger(__name__)


class DvcIgnore:
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(self, root, dirs, files):
        raise NotImplementedError


class DvcIgnorePatterns(DvcIgnore):
    def __init__(self, pattern_list, dirname, sep):
        if pattern_list:
            if isinstance(pattern_list[0], str):
                pattern_list = [
                    PatternInfo(pattern, "") for pattern in pattern_list
                ]

        self.sep = sep
        self.pattern_list = pattern_list
        self.dirname = dirname

        self.regex_pattern_list = [
            GitWildMatchPattern.pattern_to_regex(pattern_info.patterns)
            for pattern_info in pattern_list
        ]

        self.ignore_spec = [
            (ignore, re.compile("|".join(item[0] for item in group)))
            for ignore, group in groupby(
                self.regex_pattern_list, lambda x: x[1]
            )
            if ignore is not None
        ]

    @classmethod
    def from_file(cls, path, fs, name):
        assert fs.path.isabs(path)
        dirname = fs.path.normpath(fs.path.dirname(path))
        with fs.open(path, encoding="utf-8") as fobj:
            path_spec_lines = [
                PatternInfo(line, f"{name}:{line_no + 1}:{line}")
                for line_no, line in enumerate(
                    map(str.strip, fobj.readlines())
                )
                if line and not (line.strip().startswith("#"))
            ]

        return cls(path_spec_lines, dirname, fs.sep)

    def __call__(self, root: List[str], dirs: List[str], files: List[str]):
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d, True)]

        return dirs, files

    def _get_normalize_path(self, dirname, basename):
        # NOTE: `relpath` is too slow, so we have to assume that both
        # `dirname` and `self.dirname` are relative or absolute together.

        prefix = self.dirname.rstrip(self.sep) + self.sep

        if dirname == self.dirname:
            path = basename
        elif dirname.startswith(prefix):
            rel = dirname[len(prefix) :]
            # NOTE: `os.path.join` is ~x5.5 slower
            path = f"{rel}{self.sep}{basename}"
        else:
            return False

        if os.name == "nt":
            path = normalize_file(path)
        return path

    def matches(self, dirname, basename, is_dir=False, details: bool = False):
        path = self._get_normalize_path(dirname, basename)
        if not path:
            return False

        if details:
            return self._ignore_details(path, is_dir)
        return self.ignore(path, is_dir)

    def ignore(self, path, is_dir):
        def matches(pattern, path, is_dir) -> bool:
            matches_ = bool(pattern.match(path))

            if is_dir:
                matches_ |= bool(pattern.match(f"{path}/"))

            return matches_

        result = False

        for ignore, pattern in self.ignore_spec[::-1]:
            if matches(pattern, path, is_dir):
                result = ignore
                break
        return result

    def _ignore_details(self, path, is_dir: bool):
        result = []
        for (regex, _), pattern_info in list(
            zip(self.regex_pattern_list, self.pattern_list)
        ):
            # skip system pattern
            if not pattern_info.file_info:
                continue

            regex = re.compile(regex)

            matches = bool(regex.match(path))
            if is_dir:
                matches |= bool(regex.match(f"{path}/"))

            if matches:
                result.append(pattern_info.file_info)

        return result

    def __hash__(self):
        return hash(self.dirname + ":" + str(self.pattern_list))

    def __eq__(self, other):
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented
        return (self.dirname == other.dirname) & (
            [pattern.patterns for pattern in self.pattern_list]
            == [pattern.patterns for pattern in other.pattern_list]
        )

    def __bool__(self):
        return bool(self.pattern_list)


CheckIgnoreResult = namedtuple(
    "CheckIgnoreResult", ["file", "match", "patterns"]
)


def _no_match(path):
    return CheckIgnoreResult(path, False, ["::"])


class DvcIgnoreFilter:
    def __init__(self, fs, root_dir):
        from dvc.repo import Repo

        default_ignore_patterns = [
            ".hg/",
            ".git/",
            ".git",
            f"{Repo.DVC_DIR}/",
        ]

        self.fs = fs
        self.root_dir = root_dir
        self.ignores_trie_fs = Trie()
        self._ignores_trie_subrepos = Trie()

        key = self._get_key(root_dir)
        self.ignores_trie_fs[key] = DvcIgnorePatterns(
            default_ignore_patterns,
            root_dir,
            fs.sep,
        )
        self._ignores_trie_subrepos[key] = self.ignores_trie_fs[key]
        self._update(
            self.root_dir,
            self._ignores_trie_subrepos,
            dnames=None,
            ignore_subrepos=False,
        )
        self._update(
            self.root_dir,
            self.ignores_trie_fs,
            dnames=None,
            ignore_subrepos=True,
        )

    def _get_key(self, path):
        parts = self.fs.path.relparts(path, self.root_dir)
        if parts == (".",):
            parts = ()
        return parts

    def _update_trie(self, dirname: str, trie: Trie) -> None:
        key = self._get_key(dirname)
        old_pattern = trie.longest_prefix(key).value
        matches = old_pattern.matches(dirname, DvcIgnore.DVCIGNORE_FILE, False)

        path = self.fs.path.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if not matches and self.fs.exists(path):
            name = self.fs.path.relpath(path, self.root_dir)
            new_pattern = DvcIgnorePatterns.from_file(path, self.fs, name)
            if old_pattern:
                plist, prefix = merge_patterns(
                    self.fs.path.flavour,
                    old_pattern.pattern_list,
                    old_pattern.dirname,
                    new_pattern.pattern_list,
                    new_pattern.dirname,
                )
                trie[key] = DvcIgnorePatterns(plist, prefix, self.fs.sep)
            else:
                trie[key] = new_pattern
        elif old_pattern:
            trie[key] = old_pattern

    def _update(
        self,
        dirname: str,
        ignore_trie: Trie,
        dnames: Optional["List"],
        ignore_subrepos: bool,
    ) -> None:
        self._update_trie(dirname, ignore_trie)

        if ignore_subrepos:
            if dnames is None:
                try:
                    _, dnames, _ = next(self.fs.walk(dirname))
                except StopIteration:
                    dnames = []

            for dname in dnames:
                self._update_sub_repo(
                    self.fs.path.join(dirname, dname), ignore_trie
                )

    def _update_sub_repo(self, path, ignore_trie: Trie):
        from dvc.repo import Repo

        if path == self.root_dir:
            return

        dvc_dir = self.fs.path.join(path, Repo.DVC_DIR)
        if not self.fs.exists(dvc_dir):
            return

        root, dname = self.fs.path.split(path)
        key = self._get_key(root)
        pattern_info = PatternInfo(f"/{dname}/", f"in sub_repo:{dname}")
        new_pattern = DvcIgnorePatterns([pattern_info], root, self.fs.sep)
        old_pattern = ignore_trie.longest_prefix(key).value
        if old_pattern:
            plist, prefix = merge_patterns(
                self.fs.path.flavour,
                old_pattern.pattern_list,
                old_pattern.dirname,
                new_pattern.pattern_list,
                new_pattern.dirname,
            )
            ignore_trie[key] = DvcIgnorePatterns(plist, prefix, self.fs.sep)
        else:
            ignore_trie[key] = new_pattern

    def __call__(self, root, dirs, files, ignore_subrepos=True):
        abs_root = self.fs.path.abspath(root)
        ignore_pattern = self._get_trie_pattern(
            abs_root, dnames=dirs, ignore_subrepos=ignore_subrepos
        )
        if ignore_pattern:
            dirs, files = ignore_pattern(abs_root, dirs, files)
        return dirs, files

    def ls(self, fs, path, detail=True, **kwargs):
        fs_dict = {}
        dirs = []
        nondirs = []

        for entry in fs.ls(path, detail=True, **kwargs):
            name = fs.path.name(entry["name"])
            fs_dict[name] = entry
            if entry["type"] == "directory":
                dirs.append(name)
            else:
                nondirs.append(name)

        dirs, nondirs = self(path, dirs, nondirs, **kwargs)

        if not detail:
            return dirs + nondirs

        return [fs_dict[name] for name in chain(dirs, nondirs)]

    def walk(self, fs: FileSystem, path: AnyFSPath, **kwargs):
        detail = kwargs.get("detail", False)
        ignore_subrepos = kwargs.pop("ignore_subrepos", True)
        if fs.protocol == Schemes.LOCAL:
            for root, dirs, files in fs.walk(path, **kwargs):
                if detail:
                    all_dnames = set(dirs.keys())
                    all_fnames = set(files.keys())
                    dnames, fnames = self(
                        root,
                        all_dnames,
                        all_fnames,
                        ignore_subrepos=ignore_subrepos,
                    )
                    list(map(dirs.pop, all_dnames - set(dnames)))
                    list(map(files.pop, all_fnames - set(fnames)))
                else:
                    dirs[:], files[:] = self(
                        root, dirs, files, ignore_subrepos=ignore_subrepos
                    )
                yield root, dirs, files
        else:
            yield from fs.walk(path, **kwargs)

    def find(self, fs: FileSystem, path: AnyFSPath, **kwargs):
        if fs.protocol == Schemes.LOCAL:
            for root, _, files in self.walk(fs, path, **kwargs):
                for file in files:
                    # NOTE: os.path.join is ~5.5 times slower
                    yield f"{root}{fs.sep}{file}"
        else:
            yield from fs.find(path)

    def _get_trie_pattern(
        self, dirname, dnames: Optional["List"] = None, ignore_subrepos=True
    ) -> Optional["DvcIgnorePatterns"]:
        if ignore_subrepos:
            ignores_trie = self.ignores_trie_fs
        else:
            ignores_trie = self._ignores_trie_subrepos

        if not self.fs.path.isin_or_eq(dirname, self.root_dir):
            # outside of the repo
            return None

        key = self._get_key(dirname)

        ignore_pattern = ignores_trie.get(key)
        if ignore_pattern:
            return ignore_pattern

        prefix_key = ignores_trie.longest_prefix(key).key or ()
        prefix = self.fs.path.join(self.root_dir, *prefix_key)

        dirs = list(
            takewhile(
                lambda path: path != prefix,
                (parent for parent in localfs.path.parents(dirname)),
            )
        )
        dirs.reverse()
        dirs.append(dirname)

        for parent in dirs:
            self._update(parent, ignores_trie, dnames, ignore_subrepos)

        return ignores_trie.get(key)

    def _is_ignored(
        self, path: str, is_dir: bool = False, ignore_subrepos: bool = True
    ):
        if self._outside_repo(path):
            return False
        dirname, basename = self.fs.path.split(self.fs.path.normpath(path))
        ignore_pattern = self._get_trie_pattern(dirname, None, ignore_subrepos)
        if ignore_pattern:
            return ignore_pattern.matches(dirname, basename, is_dir)
        return False

    def is_ignored_dir(self, path: str, ignore_subrepos: bool = True) -> bool:
        "Only used in LocalFileSystem"
        path = self.fs.path.abspath(path)
        if path == self.root_dir:
            return False

        return self._is_ignored(path, True, ignore_subrepos=ignore_subrepos)

    def is_ignored_file(self, path: str, ignore_subrepos: bool = True) -> bool:
        "Only used in LocalFileSystem"
        path = self.fs.path.abspath(path)
        return self._is_ignored(path, False, ignore_subrepos=ignore_subrepos)

    def _outside_repo(self, path):
        return not self.fs.path.isin_or_eq(path, self.root_dir)

    def check_ignore(self, target):
        # NOTE: can only be used in `dvc check-ignore`, see
        # https://github.com/iterative/dvc/issues/5046
        full_target = self.fs.path.abspath(target)
        if not self._outside_repo(full_target):
            dirname, basename = self.fs.path.split(
                self.fs.path.normpath(full_target)
            )
            pattern = self._get_trie_pattern(dirname)
            if pattern:
                matches = pattern.matches(
                    dirname, basename, self.fs.isdir(full_target), True
                )

                if matches:
                    return CheckIgnoreResult(target, True, matches)
        return _no_match(target)

    def is_ignored(
        self, fs: FileSystem, path: str, ignore_subrepos: bool = True
    ) -> bool:
        # NOTE: can't use self.check_ignore(path).match for now, see
        # https://github.com/iterative/dvc/issues/4555
        if fs.protocol != Schemes.LOCAL:
            return False
        if fs.isfile(path):
            return self.is_ignored_file(path, ignore_subrepos)
        if fs.isdir(path):
            return self.is_ignored_dir(path, ignore_subrepos)
        return self.is_ignored_file(
            path, ignore_subrepos
        ) or self.is_ignored_dir(path, ignore_subrepos)


def init(path):
    dvcignore = os.path.join(path, DvcIgnore.DVCIGNORE_FILE)
    if os.path.exists(dvcignore):
        return dvcignore

    with open(dvcignore, "w", encoding="utf-8") as fobj:
        fobj.write(
            "# Add patterns of files dvc should ignore, which could improve\n"
            "# the performance. Learn more at\n"
            "# https://dvc.org/doc/user-guide/dvcignore\n"
        )

    return dvcignore


def destroy(path):
    from dvc.utils.fs import remove

    dvcignore = os.path.join(path, DvcIgnore.DVCIGNORE_FILE)
    remove(dvcignore)
