import os
import posixpath
from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional, TypedDict, Union

from dvc.fs.callbacks import DEFAULT_CALLBACK
from dvc.log import logger
from dvc.scm import RevError
from dvc.ui import ui
from dvc_data.index.view import DataIndexView

if TYPE_CHECKING:
    from dvc.fs.callbacks import Callback
    from dvc.repo import Repo
    from dvc.scm import Git, NoSCM
    from dvc_data.index import BaseDataIndex, DataIndex, DataIndexKey
    from dvc_data.index.diff import Change

logger = logger.getChild(__name__)


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
    old: "BaseDataIndex",
    new: "BaseDataIndex",
    *,
    granular: bool = False,
    not_in_cache: bool = False,
    callback: "Callback" = DEFAULT_CALLBACK,
    filter_keys: Optional[list["DataIndexKey"]] = None,
) -> dict[str, list[str]]:
    from dvc_data.index.diff import UNCHANGED, UNKNOWN, diff

    ret: dict[str, list[str]] = {}

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
        # The index is a trie, so even when we filter by a specific path
        # like `dir/file`, all parent nodes leading to that path (e.g., `dir/`)
        # still appear in the view. As a result, keys like `dir/` will be present
        # even if only `dir/file` matches the filter.
        # We need to skip such entries to avoid showing root of tracked directories.
        if filter_keys and not any(
            change.key[: len(filter_key)] == filter_key for filter_key in filter_keys
        ):
            continue

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

        _add_change(change.typ, change)

    return ret


class GitInfo(TypedDict, total=False):
    staged: dict[str, list[str]]
    unstaged: dict[str, list[str]]
    untracked: list[str]
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


def filter_index(
    index: Union["DataIndex", "DataIndexView"],
    filter_keys: Optional[list["DataIndexKey"]] = None,
) -> "BaseDataIndex":
    if not filter_keys:
        return index

    if isinstance(index, DataIndexView):
        orig_index = index._index
        parent_filter_fn = index.filter_fn
    else:
        orig_index = index
        parent_filter_fn = None

    def filter_fn(key: "DataIndexKey") -> bool:
        if parent_filter_fn is not None and not parent_filter_fn(key):
            return False

        for filter_key in filter_keys:
            # eg: if key is "dir/file" and filter_key is "dir/", return True
            if key[: len(filter_key)] == filter_key:
                return True
            # eg: if key is `dir/` and filter_key is `dir/file`, also return True.
            # This ensures we include parent prefixes needed to reach matching leaves.
            # Intermediate prefixes must be retained to access nested keys.
            if filter_key[: len(key)] == key:
                return True
        return False

    from dvc_data.index import view

    return view(orig_index, filter_fn=filter_fn)


def _diff_index_to_wtree(
    repo: "Repo",
    filter_keys: Optional[list["DataIndexKey"]] = None,
    granular: bool = False,
) -> dict[str, list[str]]:
    from .index import build_data_index

    with ui.progress(desc="Building workspace index", unit="entry") as pb:
        workspace = build_data_index(
            repo.index,
            repo.root_dir,
            repo.fs,
            compute_hash=True,
            callback=pb.as_callback(),
        )
        workspace_view = filter_index(workspace, filter_keys=filter_keys)

    with ui.progress(
        desc="Calculating diff between index/workspace",
        unit="entry",
    ) as pb:
        index = repo.index.data["repo"]
        view = filter_index(index, filter_keys=filter_keys)
        return _diff(
            view,
            workspace_view,
            filter_keys=filter_keys,
            granular=granular,
            not_in_cache=True,
            callback=pb.as_callback(),
        )


def _diff_head_to_index(
    repo: "Repo",
    head: str = "HEAD",
    filter_keys: Optional[list["DataIndexKey"]] = None,
    granular: bool = False,
) -> dict[str, list[str]]:
    from dvc_data.index import DataIndex

    index = repo.index.data["repo"]
    index_view = filter_index(index, filter_keys=filter_keys)

    try:
        with repo.switch(head):
            head_index = repo.index.data["repo"]
            head_view = filter_index(head_index, filter_keys=filter_keys)
    except RevError:
        logger.debug("failed to switch to '%s'", head)
        head_view = DataIndex()

    with ui.progress(desc="Calculating diff between head/index", unit="entry") as pb:
        return _diff(
            head_view,
            index_view,
            filter_keys=filter_keys,
            granular=granular,
            callback=pb.as_callback(),
        )


class Status(TypedDict):
    not_in_cache: list[str]
    not_in_remote: list[str]
    committed: dict[str, list[str]]
    uncommitted: dict[str, list[str]]
    untracked: list[str]
    unchanged: list[str]
    git: GitInfo


def _transform_git_paths_to_dvc(repo: "Repo", files: Iterable[str]) -> list[str]:
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


def _get_entries_not_in_remote(
    repo: "Repo",
    filter_keys: Optional[list["DataIndexKey"]] = None,
    granular: bool = False,
    remote_refresh: bool = False,
) -> list[str]:
    """Get entries that are not in remote storage."""
    from dvc.repo.worktree import worktree_view
    from dvc_data.index import StorageKeyError

    # View into the index, with only pushable entries
    index = worktree_view(repo.index, push=True)
    data_index = index.data["repo"]

    view = filter_index(data_index, filter_keys=filter_keys)  # type: ignore[arg-type]

    missing_entries = []
    with ui.progress(desc="Checking remote", unit="entry") as pb:
        for key, entry in view.iteritems(shallow=not granular):
            if not (entry and entry.hash_info):
                continue

            # The index is a trie, so even when we filter by a specific path
            # like `dir/file`, all parent nodes leading to that path (e.g., `dir/`)
            # still appear in the view. As a result, keys like `dir/` will be present
            # even if only `dir/file` matches the filter.
            # We need to skip such entries to avoid showing root of tracked directories.
            if filter_keys and not any(
                key[: len(filter_key)] == filter_key for filter_key in filter_keys
            ):
                continue

            k = (*key, "") if entry.meta and entry.meta.isdir else key
            try:
                if not view.storage_map.remote_exists(entry, refresh=remote_refresh):
                    missing_entries.append(os.path.sep.join(k))
                    pb.update()
            except StorageKeyError:
                pass

    return missing_entries


def _matches_target(p: str, targets: Iterable[str]) -> bool:
    sep = os.sep
    return any(p == t or p.startswith(t + sep) for t in targets)


def _prune_keys(filter_keys: list["DataIndexKey"]) -> list["DataIndexKey"]:
    sorted_keys = sorted(set(filter_keys), key=len)
    result: list[DataIndexKey] = []

    for key in sorted_keys:
        if not any(key[: len(prefix)] == prefix for prefix in result):
            result.append(key)
    return result


def status(
    repo: "Repo",
    targets: Optional[Iterable[Union[os.PathLike[str], str]]] = None,
    *,
    granular: bool = False,
    untracked_files: str = "no",
    not_in_remote: bool = False,
    remote_refresh: bool = False,
    head: str = "HEAD",
) -> Status:
    from dvc.scm import NoSCMError, SCMError

    targets = targets or []
    filter_keys: list[DataIndexKey] = [repo.fs.relparts(os.fspath(t)) for t in targets]
    # try to remove duplicate and overlapping keys
    filter_keys = _prune_keys(filter_keys)

    uncommitted_diff = _diff_index_to_wtree(
        repo, filter_keys=filter_keys, granular=granular
    )
    unchanged = set(uncommitted_diff.pop("unchanged", []))
    entries_not_in_remote = (
        _get_entries_not_in_remote(
            repo,
            filter_keys=filter_keys,
            granular=granular,
            remote_refresh=remote_refresh,
        )
        if not_in_remote
        else []
    )

    try:
        committed_diff = _diff_head_to_index(
            repo, filter_keys=filter_keys, head=head, granular=granular
        )
    except (SCMError, NoSCMError):
        committed_diff = {}
    else:
        unchanged &= set(committed_diff.pop("unchanged", []))

    git_info = _git_info(repo.scm, untracked_files=untracked_files)
    scm_filter_targets = {
        os.path.relpath(os.path.abspath(t), repo.scm.root_dir) for t in targets
    }
    untracked_it: Iterable[str] = git_info.get("untracked", [])
    if scm_filter_targets:
        untracked_it = (
            f for f in untracked_it if _matches_target(f, scm_filter_targets)
        )
    untracked = _transform_git_paths_to_dvc(repo, untracked_it)
    # order matters here
    return Status(
        not_in_cache=uncommitted_diff.pop("not_in_cache", []),
        not_in_remote=entries_not_in_remote,
        committed=committed_diff,
        uncommitted=uncommitted_diff,
        untracked=untracked,
        unchanged=list(unchanged),
        git=git_info,
    )
