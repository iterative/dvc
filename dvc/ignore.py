import functools
import os
import re
from collections.abc import Iterable, Iterator
from itertools import chain, groupby, takewhile
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Optional, Union, overload

from pathspec.patterns import GitWildMatchPattern
from pathspec.util import normalize_file
from pygtrie import Trie

from dvc.fs import Schemes, localfs
from dvc.log import logger
from dvc.pathspec_math import PatternInfo, merge_patterns

if TYPE_CHECKING:
    from typing_extensions import Self

    from dvc.fs import FileSystem

logger = logger.getChild(__name__)


class DvcIgnore:
    DVCIGNORE_FILE = ".dvcignore"

    def __call__(
        self, root: str, dirs: list[str], files: list[str]
    ) -> tuple[list[str], list[str]]:
        raise NotImplementedError


class DvcIgnorePatterns(DvcIgnore):
    def __init__(
        self, pattern_list: Iterable[Union[PatternInfo, str]], dirname: str, sep: str
    ) -> None:
        from pathspec.patterns.gitwildmatch import _DIR_MARK

        pattern_infos = [
            pattern if isinstance(pattern, PatternInfo) else PatternInfo(pattern, "")
            for pattern in pattern_list
        ]

        self.sep = sep
        self.pattern_list: list[PatternInfo] = []
        self.dirname = dirname
        self.find_matching_pattern = functools.cache(self._find_matching_pattern)

        regex_pattern_list: list[tuple[str, bool, bool, PatternInfo]] = []
        for count, pattern_info in enumerate(pattern_infos):
            regex, ignore = GitWildMatchPattern.pattern_to_regex(pattern_info.patterns)
            if regex is not None and ignore is not None:
                self.pattern_list.append(pattern_info)
                regex = regex.replace(f"<{_DIR_MARK}>", f"<{_DIR_MARK}{count}>")
                regex_pattern_list.append(
                    (regex, ignore, pattern_info.patterns.endswith("/"), pattern_info)
                )

        def keyfunc(item: tuple[str, bool, bool, PatternInfo]) -> tuple[bool, bool]:
            _, ignore, dir_only_pattern, _ = item
            return ignore, dir_only_pattern

        self.ignore_spec: list[
            tuple[
                re.Pattern[str],
                bool,
                bool,
                dict[Optional[str], tuple[str, PatternInfo]],
            ]
        ]
        self.ignore_spec = []
        for (ignore, dir_only_pattern), group in groupby(
            regex_pattern_list, key=keyfunc
        ):
            if ignore:
                # For performance, we combine all exclude patterns.
                # But we still need to figure out which pattern matched which rule,
                # (eg: to show in `dvc check-ignore`).
                # So, we use named groups and keep a map of group name to pattern.
                pattern_map: dict[Optional[str], tuple[str, PatternInfo]] = {
                    f"rule_{i}": (regex, pi)
                    for i, (regex, _, _, pi) in enumerate(group)
                }
                combined_regex = "|".join(
                    f"(?P<{name}>{regex})" for name, (regex, _) in pattern_map.items()
                )
                self.ignore_spec.append(
                    (re.compile(combined_regex), ignore, dir_only_pattern, pattern_map)
                )
            else:
                # unignored patterns are not combined with `|`.
                for regex, _, _, pi in group:
                    pattern_map = {None: (regex, pi)}
                    self.ignore_spec.append(
                        (re.compile(regex), ignore, dir_only_pattern, pattern_map)
                    )

    @classmethod
    def from_file(cls, path: str, fs: "FileSystem", name: str) -> "Self":
        assert fs.isabs(path)
        dirname = fs.normpath(fs.dirname(path))
        with fs.open(path, encoding="utf-8") as fobj:
            path_spec_lines = [
                PatternInfo(line, f"{name}:{line_no + 1}:{line}")
                for line_no, line in enumerate(map(str.strip, fobj.readlines()))
                if line and not (line.strip().startswith("#"))
            ]

        return cls(path_spec_lines, dirname, fs.sep)

    def __call__(
        self, root: str, dirs: list[str], files: list[str]
    ) -> tuple[list[str], list[str]]:
        files = [f for f in files if not self.matches(root, f)]
        dirs = [d for d in dirs if not self.matches(root, d, True)]

        return dirs, files

    def _get_normalize_path(self, dirname: str, basename: str) -> Optional[str]:
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
            return None

        if os.name == "nt":
            return normalize_file(path)
        return path

    @overload
    def matches(
        self,
        dirname: str,
        basename: str,
        is_dir: bool = False,
        details: Literal[False] = ...,
    ) -> bool: ...

    @overload
    def matches(
        self,
        dirname: str,
        basename: str,
        is_dir: bool = False,
        details: Literal[True] = ...,
    ) -> tuple[bool, list[PatternInfo]]: ...

    @overload
    def matches(
        self,
        dirname: str,
        basename: str,
        is_dir: bool = False,
        details: bool = False,
    ) -> Union[bool, tuple[bool, list[PatternInfo]]]: ...

    def matches(
        self,
        dirname: str,
        basename: str,
        is_dir: bool = False,
        details: bool = False,
    ) -> Union[bool, tuple[bool, list[PatternInfo]]]:
        path = self._get_normalize_path(dirname, basename)
        result = False
        _match: list[PatternInfo] = []
        if path:
            result, _match = self._ignore(path, is_dir)
        return (result, _match) if details else result

    def _find_matching_pattern(
        self, path: str, is_dir: bool
    ) -> tuple[bool, list[PatternInfo]]:
        paths = [path]
        if is_dir and not path.endswith("/"):
            paths.append(f"{path}/")

        for pattern, ignore, dir_only_pattern, pattern_map in reversed(
            self.ignore_spec
        ):
            if dir_only_pattern and not is_dir:
                continue
            for p in paths:
                match = pattern.match(p)
                if not match:
                    continue
                if ignore:
                    group_name, _match = next(
                        (
                            (name, _match)
                            for name, _match in match.groupdict().items()
                            if name.startswith("rule_") and _match is not None
                        )
                    )
                else:
                    # unignored patterns are not combined with `|`,
                    # so there are no groups.
                    group_name = None
                _regex, pattern_info = pattern_map[group_name]
                return ignore, [pattern_info]
        return False, []

    def _ignore(self, path: str, is_dir: bool) -> tuple[bool, list[PatternInfo]]:
        parts = path.split("/")
        result = False
        matches: list[PatternInfo] = []
        for i in range(1, len(parts) + 1):
            rel_path = "/".join(parts[:i])
            result, _matches = self.find_matching_pattern(
                rel_path, is_dir or i < len(parts)
            )
            if i < len(parts) and not result:
                continue
            matches.extend(_matches)
            if result:
                break
        return result, matches

    def __hash__(self) -> int:
        return hash(self.dirname + ":" + str(self.pattern_list))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DvcIgnorePatterns):
            return NotImplemented
        return (self.dirname == other.dirname) & (
            [pattern.patterns for pattern in self.pattern_list]
            == [pattern.patterns for pattern in other.pattern_list]
        )

    def __bool__(self) -> bool:
        return bool(self.pattern_list)


class CheckIgnoreResult(NamedTuple):
    file: str
    match: bool
    pattern_infos: list[PatternInfo]


class DvcIgnoreFilter:
    def __init__(self, fs: "FileSystem", root_dir: str) -> None:
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

    def _get_key(self, path: str) -> tuple[str, ...]:
        parts = self.fs.relparts(path, self.root_dir)
        if parts == (os.curdir,):
            return ()
        return parts

    def _update_trie(self, dirname: str, trie: Trie) -> None:
        key = self._get_key(dirname)
        old_pattern = trie.longest_prefix(key).value
        matches = old_pattern.matches(dirname, DvcIgnore.DVCIGNORE_FILE, False)

        path = self.fs.join(dirname, DvcIgnore.DVCIGNORE_FILE)
        if not matches and self.fs.exists(path):
            name = self.fs.relpath(path, self.root_dir)
            new_pattern = DvcIgnorePatterns.from_file(path, self.fs, name)
            if old_pattern:
                plist, prefix = merge_patterns(
                    self.fs.flavour,
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
        dnames: Optional["list"],
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
                self._update_sub_repo(self.fs.join(dirname, dname), ignore_trie)

    def _update_sub_repo(self, path: str, ignore_trie: Trie) -> None:
        from dvc.repo import Repo

        if path == self.root_dir:
            return

        dvc_dir = self.fs.join(path, Repo.DVC_DIR)
        if not self.fs.exists(dvc_dir):
            return

        root, dname = self.fs.split(path)
        key = self._get_key(root)
        pattern_info = PatternInfo(f"/{dname}/", f"in sub_repo:{dname}")
        new_pattern = DvcIgnorePatterns([pattern_info], root, self.fs.sep)
        old_pattern = ignore_trie.longest_prefix(key).value
        if old_pattern:
            plist, prefix = merge_patterns(
                self.fs.flavour,
                old_pattern.pattern_list,
                old_pattern.dirname,
                new_pattern.pattern_list,
                new_pattern.dirname,
            )
            ignore_trie[key] = DvcIgnorePatterns(plist, prefix, self.fs.sep)
        else:
            ignore_trie[key] = new_pattern

    def __call__(
        self, root: str, dirs: list[str], files: list[str], ignore_subrepos: bool = True
    ) -> tuple[list[str], list[str]]:
        abs_root = self.fs.abspath(root)
        ignore_pattern = self._get_trie_pattern(
            abs_root, dnames=dirs, ignore_subrepos=ignore_subrepos
        )
        if ignore_pattern:
            dirs, files = ignore_pattern(abs_root, dirs, files)
        return dirs, files

    @overload
    def ls(
        self, fs: "FileSystem", path: str, detail: Literal[True], **kwargs: Any
    ) -> list[dict[str, Any]]: ...

    @overload
    def ls(
        self, fs: "FileSystem", path: str, detail: Literal[False], **kwargs
    ) -> list[str]: ...

    @overload
    def ls(
        self, fs: "FileSystem", path: str, detail: bool = True, **kwargs
    ) -> Union[list[str], list[dict[str, Any]]]: ...

    def ls(
        self, fs: "FileSystem", path: str, detail: bool = True, **kwargs: Any
    ) -> Union[list[str], list[dict[str, Any]]]:
        fs_dict = {}
        dirs = []
        nondirs = []

        for entry in fs.ls(path, detail=True, **kwargs):
            name = fs.name(entry["name"])
            fs_dict[name] = entry
            if entry["type"] == "directory":
                dirs.append(name)
            else:
                nondirs.append(name)

        dirs, nondirs = self(path, dirs, nondirs, **kwargs)

        if not detail:
            return dirs + nondirs

        return [fs_dict[name] for name in chain(dirs, nondirs)]

    def walk(
        self, fs: "FileSystem", path: str, **kwargs: Any
    ) -> Iterator[
        Union[
            tuple[str, list[str], list[str]],
            tuple[str, dict[str, dict], dict[str, dict]],
        ]
    ]:
        detail = kwargs.get("detail", False)
        ignore_subrepos = kwargs.pop("ignore_subrepos", True)
        if fs.protocol == Schemes.LOCAL:
            for root, dirs, files in fs.walk(path, **kwargs):
                if detail:
                    assert isinstance(dirs, dict)
                    assert isinstance(files, dict)
                    dnames, fnames = self(
                        root,
                        list(dirs),
                        list(files),
                        ignore_subrepos=ignore_subrepos,
                    )
                    list(map(dirs.pop, dirs.keys() - set(dnames)))
                    list(map(files.pop, files.keys() - set(fnames)))
                else:
                    dirs[:], files[:] = self(
                        root, dirs, files, ignore_subrepos=ignore_subrepos
                    )
                yield root, dirs, files
        else:
            yield from fs.walk(path, **kwargs)

    def find(self, fs: "FileSystem", path: str, **kwargs: Any) -> Iterator[str]:
        if fs.protocol == Schemes.LOCAL:
            for root, _, files in self.walk(fs, path, **kwargs):
                for file in files:
                    # NOTE: os.path.join is ~5.5 times slower
                    yield f"{root}{fs.sep}{file}"
        else:
            yield from fs.find(path)

    def _get_trie_pattern(
        self, dirname: str, dnames: Optional[list[str]] = None, ignore_subrepos=True
    ) -> Optional["DvcIgnorePatterns"]:
        if ignore_subrepos:
            ignores_trie = self.ignores_trie_fs
        else:
            ignores_trie = self._ignores_trie_subrepos

        if not self.fs.isin_or_eq(dirname, self.root_dir):
            # outside of the repo
            return None

        key = self._get_key(dirname)

        ignore_pattern = ignores_trie.get(key)
        if ignore_pattern:
            return ignore_pattern

        prefix_key = ignores_trie.longest_prefix(key).key or ()
        prefix = self.fs.join(self.root_dir, *prefix_key)

        dirs = list(
            takewhile(
                lambda path: path != prefix,
                (parent for parent in localfs.parents(dirname)),
            )
        )
        dirs.reverse()
        dirs.append(dirname)

        for parent in dirs:
            self._update(parent, ignores_trie, dnames, ignore_subrepos)

        return ignores_trie.get(key)

    def _is_ignored(
        self, path: str, is_dir: bool = False, ignore_subrepos: bool = True
    ) -> bool:
        if self._outside_repo(path):
            return False
        dirname, basename = self.fs.split(self.fs.normpath(path))
        ignore_pattern = self._get_trie_pattern(dirname, None, ignore_subrepos)
        if ignore_pattern:
            return ignore_pattern.matches(dirname, basename, is_dir)
        return False

    def is_ignored_dir(self, path: str, ignore_subrepos: bool = True) -> bool:
        # only used in LocalFileSystem
        path = self.fs.abspath(path)
        if path == self.root_dir:
            return False

        return self._is_ignored(path, True, ignore_subrepos=ignore_subrepos)

    def is_ignored_file(self, path: str, ignore_subrepos: bool = True) -> bool:
        # only used in LocalFileSystem
        path = self.fs.abspath(path)
        return self._is_ignored(path, False, ignore_subrepos=ignore_subrepos)

    def _outside_repo(self, path: str) -> bool:
        return not self.fs.isin_or_eq(path, self.root_dir)

    def check_ignore(self, target: str) -> CheckIgnoreResult:
        # NOTE: can only be used in `dvc check-ignore`, see
        # https://github.com/iterative/dvc/issues/5046
        full_target = self.fs.abspath(target)
        matched_patterns: list[PatternInfo] = []
        ignore = False
        if not self._outside_repo(full_target):
            dirname, basename = self.fs.split(self.fs.normpath(full_target))
            pattern = self._get_trie_pattern(dirname)
            if pattern:
                ignore, matched_patterns = pattern.matches(
                    dirname, basename, self.fs.isdir(full_target), details=True
                )
        return CheckIgnoreResult(target, ignore, matched_patterns)

    def is_ignored(
        self, fs: "FileSystem", path: str, ignore_subrepos: bool = True
    ) -> bool:
        # NOTE: can't use self.check_ignore(path).match for now, see
        # https://github.com/iterative/dvc/issues/4555
        if fs.protocol != Schemes.LOCAL:
            return False
        if fs.isfile(path):
            return self.is_ignored_file(path, ignore_subrepos)
        if fs.isdir(path):
            return self.is_ignored_dir(path, ignore_subrepos)
        return self.is_ignored_file(path, ignore_subrepos) or self.is_ignored_dir(
            path, ignore_subrepos
        )


def init(path: Union[str, os.PathLike[str]]) -> str:
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


def destroy(path: Union[str, os.PathLike[str]]) -> None:
    from dvc.utils.fs import remove

    dvcignore = os.path.join(path, DvcIgnore.DVCIGNORE_FILE)
    remove(dvcignore)
