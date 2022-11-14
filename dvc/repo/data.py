import os
import posixpath
from collections import defaultdict
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    TypedDict,
    Union,
    cast,
)

from dvc.fs.git import GitFileSystem
from dvc.ui import ui

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.scm import Git, NoSCM
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.obj import HashFile


def posixpath_to_os_path(path: str) -> str:
    return path.replace(posixpath.sep, os.path.sep)


def _in_cache(obj: "HashInfo", cache: "HashFileDB") -> bool:
    from dvc_objects.errors import ObjectFormatError

    assert obj.value
    try:
        cache.check(obj.value)
        return True
    except (FileNotFoundError, ObjectFormatError):
        return False


def _shallow_diff(
    root: str,
    old: Optional["HashInfo"],
    new: Optional["HashInfo"],
    cache: "HashFileDB",
) -> Dict[str, List[str]]:
    d = {}

    old_root = new_root = root
    if old and old.isdir:
        old_root = os.path.sep.join([root, ""])
    if new and new.isdir:
        new_root = os.path.sep.join([root, ""])

    if old and not _in_cache(old, cache):
        d["not_in_cache"] = [old_root]

    if not old and not new:
        return d
    if not new:
        return {"deleted": [old_root], **d}
    if not old:
        return {"added": [new_root], **d}
    if old != new:
        return {"modified": [new_root], **d}
    return {"unchanged": [new_root], **d}


def _granular_diff(
    root: str,
    old_obj: Optional["HashFile"],
    new_obj: Optional["HashFile"],
    cache: "HashFileDB",
) -> Dict[str, List[str]]:
    from dvc_data.hashfile.diff import ROOT
    from dvc_data.hashfile.diff import diff as odiff

    def path_join(root: str, *paths: str, isdir: bool = False) -> str:
        if not isdir and paths == ROOT:
            return root
        return os.path.sep.join([root, *paths])

    diff_data = odiff(old_obj, new_obj, cache)

    output: Dict[str, List[str]] = defaultdict(list)
    for state in ("added", "deleted", "modified", "unchanged"):
        items = getattr(diff_data, state)
        for item in items:  # pylint: disable=not-an-iterable
            entry = item.old if state == "deleted" else item.new
            isdir = entry.oid.isdir if entry.oid else False

            path = path_join(root, *entry.key, isdir=isdir)
            output[state].append(path)
            if not item.old.in_cache and state != "added":
                output["not_in_cache"].append(path)
    return dict(output)


def _get_obj_items(root: str, obj: Optional["HashFile"]) -> List[str]:
    if not obj:
        return []

    from dvc_data.hashfile.tree import Tree

    sep = os.path.sep
    if isinstance(obj, Tree):
        return [sep.join([root, *key]) for key, _, _ in obj]
    return [root]


def _diff(
    root: str,
    old_oid: Optional["HashInfo"],
    old_obj: Optional["HashFile"],
    new_oid: Optional["HashInfo"],
    new_obj: Optional["HashFile"],
    odb: "HashFileDB",
    granular: bool = False,
) -> Dict[str, List[str]]:
    if not granular:
        return _shallow_diff(root, old_oid, new_oid, odb)
    if (old_oid and not old_obj) or (new_oid and not new_obj):
        # we don't have enough information to give full details
        unknown = _get_obj_items(root, new_obj)
        shallow_diff = _shallow_diff(root, old_oid, new_oid, odb)
        return {**shallow_diff, "unknown": unknown}
    return _granular_diff(root, old_obj, new_obj, odb)


class GitInfo(TypedDict, total=False):
    staged: Dict[str, List[str]]
    unstaged: Dict[str, List[str]]
    untracked: List[str]
    is_empty: bool
    is_dirty: bool


def _git_info(
    scm: Union["Git", "NoSCM"], untracked_files: str = "all"
) -> GitInfo:
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
    from dvc_data.hashfile.build import build

    unstaged_diff = defaultdict(list)
    for out in repo.index.outs:
        out = cast("Output", out)
        if not out.use_cache:
            continue

        try:
            _, _, new = build(
                out.odb,
                out.fs_path,
                out.fs,
                out.fs.PARAM_CHECKSUM,
                ignore=out.dvcignore,
                dry_run=True,
            )
        except FileNotFoundError:
            new = None

        cache = repo.odb.repo
        root = str(out)
        old = out.get_obj()

        with ui.status(f"Calculating diff for {root} between index/workspace"):
            d = _diff(
                root,
                out.hash_info,
                old,
                new.hash_info if new else None,
                new,
                cache,
                **kwargs,
            )

        for state, items in d.items():
            if not items:
                continue
            unstaged_diff[state].extend(items)
    return unstaged_diff


def _diff_head_to_index(
    repo: "Repo", head: str = "HEAD", **kwargs: Any
) -> Dict[str, List[str]]:
    # we need to store objects from index and the HEAD to diff later
    objs: Dict[str, Dict[str, Tuple["HashFile", "HashInfo"]]]
    objs = defaultdict(dict)

    staged_diff = defaultdict(list)
    for rev in repo.brancher(revs=[head]):
        for out in repo.index.outs:
            out = cast("Output", out)
            if not out.use_cache:
                continue

            root = str(out)
            if isinstance(out.fs, GitFileSystem):
                root = posixpath_to_os_path(root)
            typ = "index" if rev == "workspace" else head
            objs[root][typ] = (out.get_obj(), out.hash_info)

    cache = repo.odb.repo
    for root, obj_d in objs.items():
        old_obj, old_oid = obj_d.get(head, (None, None))
        new_obj, new_oid = obj_d.get("index", (None, None))
        with ui.status(f"Calculating diff for {root} between head/index"):
            d = _diff(
                root, old_oid, old_obj, new_oid, new_obj, cache, **kwargs
            )

        for state, items in d.items():
            if not items:
                continue
            staged_diff[state].extend(items)

    return staged_diff


class Status(TypedDict):
    not_in_cache: List[str]
    committed: Dict[str, List[str]]
    uncommitted: Dict[str, List[str]]
    untracked: List[str]
    unchanged: List[str]
    git: GitInfo


def _transform_git_paths_to_dvc(
    repo: "Repo", files: Iterable[str]
) -> List[str]:
    """Transform files rel. to Git root to DVC root, and drop outside files."""
    rel = repo.fs.path.relpath(repo.root_dir, repo.scm.root_dir).rstrip("/")

    # if we have repo root in a different location than scm's root,
    # i.e. subdir repo, all git_paths need to be transformed rel. to the DVC
    # repo root and anything outside need to be filtered out.
    if rel not in (os.curdir, ""):
        prefix = rel + os.sep
        length = len(prefix)
        files = (file[length:] for file in files if file.startswith(prefix))

    start = repo.fs.path.relpath(repo.fs.path.getcwd(), repo.root_dir)
    if start in (os.curdir, ""):
        return list(files)
    # we need to convert repo relative paths to curdir relative.
    return [repo.fs.path.relpath(file, start) for file in files]


def status(repo: "Repo", untracked_files: str = "no", **kwargs: Any) -> Status:
    from dvc.scm import NoSCMError, SCMError

    head = kwargs.pop("head", "HEAD")
    uncommitted_diff = _diff_index_to_wtree(repo, **kwargs)
    not_in_cache = uncommitted_diff.pop("not_in_cache", [])
    unchanged = set(uncommitted_diff.pop("unchanged", []))

    try:
        committed_diff = _diff_head_to_index(repo, head=head, **kwargs)
    except (SCMError, NoSCMError):
        committed_diff = {}
    else:
        # we don't care about not-in-cache between the head and the index.
        committed_diff.pop("not_in_cache", None)
        unchanged &= set(committed_diff.pop("unchanged", []))

    git_info = _git_info(repo.scm, untracked_files=untracked_files)
    untracked = git_info.get("untracked", [])
    untracked = _transform_git_paths_to_dvc(repo, untracked)
    # order matters here
    return Status(
        not_in_cache=not_in_cache,
        committed=committed_diff,
        uncommitted=uncommitted_diff,
        untracked=untracked,
        unchanged=list(unchanged),
        git=git_info,
    )


def ls(
    repo: "Repo",
    targets: List[Optional[str]] = None,
    recursive: bool = False,
) -> Iterator[Dict[str, Any]]:
    targets = targets or [None]
    pairs = chain.from_iterable(
        repo.stage.collect_granular(target, recursive=recursive)
        for target in targets
    )
    for stage, filter_info in pairs:
        for out in stage.filter_outs(filter_info):
            yield {"path": str(out), **out.annot.to_dict()}
