import os
import posixpath
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, TypedDict, Union

from dvc.fs.callbacks import DEFAULT_CALLBACK
from dvc.ui import ui

if TYPE_CHECKING:
    from dvc.fs.callbacks import Callback
    from dvc.repo import Repo
    from dvc.scm import Git, NoSCM
    from dvc_data.index import DataIndex
    from dvc_data.index.diff import Change


def posixpath_to_os_path(path: str) -> str:
    return path.replace(posixpath.sep, os.path.sep)


def _adapt_typ(typ: str) -> str:
    from dvc_data.index.diff import ADD, DELETE, MODIFY

    if typ == MODIFY:
        return "modified"

    if typ == ADD:
        return "added"

    if typ == DELETE:
        return "deleted"

    return typ


def _adapt_path(change: "Change") -> str:
    isdir = False
    if change.new and change.new.meta:
        isdir = change.new.meta.isdir
    elif change.old and change.old.meta:
        isdir = change.old.meta.isdir
    key = change.key
    if isdir:
        key = (*key, "")
    return os.path.sep.join(key)


def _diff(
    old: "DataIndex",
    new: "DataIndex",
    *,
    granular: bool = False,
    not_in_cache: bool = False,
    not_in_remote: bool = False,
    remote_refresh: bool = False,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> Dict[str, List[str]]:
    from dvc_data.index import StorageError
    from dvc_data.index.diff import UNCHANGED, UNKNOWN, diff

    ret: Dict[str, List[str]] = {}

    def _add_change(typ, change):
        typ = _adapt_typ(typ)
        if typ not in ret:
            ret[typ] = []

        ret[typ].append(_adapt_path(change))

    for change in diff(
        old,
        new,
        with_unchanged=True,
        shallow=not granular,
        hash_only=True,
        with_unknown=True,
        callback=callback,
    ):
        if (
            change.typ == UNCHANGED
            and (not change.old or not change.old.hash_info)
            and (not change.new or not change.new.hash_info)
        ):
            # NOTE: emulating previous behaviour
            continue

        if change.typ == UNKNOWN and not change.new:
            # NOTE: emulating previous behaviour
            continue

        if (
            not_in_cache
            and change.old
            and change.old.hash_info
            and not old.storage_map.cache_exists(change.old)
        ):
            # NOTE: emulating previous behaviour
            _add_change("not_in_cache", change)

        try:
            if (
                not_in_remote
                and change.old
                and change.old.hash_info
                and not old.storage_map.remote_exists(
                    change.old, refresh=remote_refresh
                )
            ):
                _add_change("not_in_remote", change)
        except StorageError:
            pass

        _add_change(change.typ, change)

    return ret


class GitInfo(TypedDict, total=False):
    staged: Dict[str, List[str]]
    unstaged: Dict[str, List[str]]
    untracked: List[str]
    is_empty: bool
    is_dirty: bool


def _git_info(scm: Union["Git", "NoSCM"], untracked_files: str = "all") -> GitInfo:
    from scmrepo.exceptions import SCMError

    from dvc.scm import NoSCM

    if isinstance(scm, NoSCM):
        return {}

    try:
        scm.get_rev()
    except SCMError:
        empty_repo = True
    else:
        empty_repo = False

    staged, unstaged, untracked = scm.status(untracked_files=untracked_files)
    if os.name == "nt":
        untracked = [posixpath_to_os_path(path) for path in untracked]
    # NOTE: order is important here.
    return GitInfo(
        staged=staged,
        unstaged=unstaged,
        untracked=untracked,
        is_empty=empty_repo,
        is_dirty=any([staged, unstaged, untracked]),
    )


def _diff_index_to_wtree(repo: "Repo", **kwargs: Any) -> Dict[str, List[str]]:
    from .index import build_data_index

    with ui.progress(
        desc="Building workspace index",
        unit="entry",
    ) as pb:
        workspace = build_data_index(
            repo.index,
            repo.root_dir,
            repo.fs,
            compute_hash=True,
            callback=pb.as_callback(),
        )

    with ui.progress(
        desc="Calculating diff between index/workspace",
        unit="entry",
    ) as pb:
        return _diff(
            repo.index.data["repo"],
            workspace,
            not_in_cache=True,
            callback=pb.as_callback(),
            **kwargs,
        )


def _diff_head_to_index(
    repo: "Repo", head: str = "HEAD", **kwargs: Any
) -> Dict[str, List[str]]:
    index = repo.index.data["repo"]

    with repo.switch(head):
        head_index = repo.index.data["repo"]

    with ui.progress(
        desc="Calculating diff between head/index",
        unit="entry",
    ) as pb:
        return _diff(head_index, index, callback=pb.as_callback(), **kwargs)


class Status(TypedDict):
    not_in_cache: List[str]
    not_in_remote: List[str]
    committed: Dict[str, List[str]]
    uncommitted: Dict[str, List[str]]
    untracked: List[str]
    unchanged: List[str]
    git: GitInfo


def _transform_git_paths_to_dvc(repo: "Repo", files: Iterable[str]) -> List[str]:
    """Transform files rel. to Git root to DVC root, and drop outside files."""
    rel = repo.fs.relpath(repo.root_dir, repo.scm.root_dir).rstrip("/")

    # if we have repo root in a different location than scm's root,
    # i.e. subdir repo, all git_paths need to be transformed rel. to the DVC
    # repo root and anything outside need to be filtered out.
    if rel not in (os.curdir, ""):
        prefix = rel + os.sep
        length = len(prefix)
        files = (file[length:] for file in files if file.startswith(prefix))

    start = repo.fs.relpath(repo.fs.getcwd(), repo.root_dir)
    if start in (os.curdir, ""):
        return list(files)
    # we need to convert repo relative paths to curdir relative.
    return [repo.fs.relpath(file, start) for file in files]


def status(
    repo: "Repo",
    untracked_files: str = "no",
    **kwargs: Any,
) -> Status:
    from dvc.scm import NoSCMError, SCMError

    head = kwargs.pop("head", "HEAD")
    uncommitted_diff = _diff_index_to_wtree(
        repo,
        **kwargs,
    )
    unchanged = set(uncommitted_diff.pop("unchanged", []))

    try:
        committed_diff = _diff_head_to_index(repo, head=head, **kwargs)
    except (SCMError, NoSCMError):
        committed_diff = {}
    else:
        unchanged &= set(committed_diff.pop("unchanged", []))

    git_info = _git_info(repo.scm, untracked_files=untracked_files)
    untracked = git_info.get("untracked", [])
    untracked = _transform_git_paths_to_dvc(repo, untracked)
    # order matters here
    return Status(
        not_in_cache=uncommitted_diff.pop("not_in_cache", []),
        not_in_remote=uncommitted_diff.pop("not_in_remote", []),
        committed=committed_diff,
        uncommitted=uncommitted_diff,
        untracked=untracked,
        unchanged=list(unchanged),
        git=git_info,
    )
