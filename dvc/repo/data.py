import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, cast

from dvc.ui import ui

if TYPE_CHECKING:
    from scmrepo.base import Base

    from dvc.output import Output
    from dvc.repo import Repo
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.obj import HashFile


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
    if old.isdir != new.isdir:
        return {"deleted": [old_root], "added": [new_root], **d}

    assert old_root == new_root
    root = old_root  # the root are the same
    if old != new:
        assert old.isdir == new.isdir
        return {"modified": [root], **d}
    return {"unchanged": [root], **d}


def _granular_diff(
    root: str,
    old_obj: Optional["HashFile"],
    new_obj: Optional["HashFile"],
    cache: "HashFileDB",
    with_dirs: bool = False,
) -> Dict[str, List[str]]:
    from dvc_data.diff import ROOT
    from dvc_data.diff import diff as odiff
    from dvc_data.objects.tree import Tree

    drop_root = False
    trees = isinstance(old_obj, Tree) or isinstance(new_obj, Tree)
    if trees:
        drop_root = not with_dirs

    def path_join(root: str, *paths: str) -> str:
        if not trees and paths == ROOT:
            return root
        return os.path.sep.join([root, *paths])

    diff_data = odiff(old_obj, new_obj, cache)

    output: Dict[str, List[str]] = defaultdict(list)
    for state in ("added", "deleted", "modified", "unchanged"):
        items = getattr(diff_data, state)
        output[state].extend(
            path_join(root, *item.new.key)
            for item in items  # pylint: disable=not-an-iterable
            if not (drop_root and item.new.key == ROOT)
        )
        # TODO: PERF: diff is checking not_in_cache for each even if we only
        # need it for the index.
        # BUG: not_in_cache file also shows up as modified in staged and
        # unstaged. We currently don't know if it is really modified.
        output["not_in_cache"].extend(
            path_join(root, *item.new.key)
            for item in items  # pylint: disable=not-an-iterable
            if not item.old.in_cache
            and not (drop_root and item.new.key == ROOT)
            and state != "added"
        )
    return output


def _granular_diff_oid(
    root,
    old_oid: Optional["HashInfo"],
    old_obj: Optional["HashFile"],
    new_oid: Optional["HashInfo"],
    new_obj: Optional["HashFile"],
    cache: "HashFileDB",
    with_dirs: bool = False,
    **kwargs: Any,
):
    return _granular_diff(root, old_obj, new_obj, cache, with_dirs=with_dirs)


class GitInfo(TypedDict, total=False):
    staged: Dict[str, List[str]]
    unstaged: Dict[str, List[str]]
    untracked: List[str]
    is_empty: bool
    is_dirty: bool


def _git_info(scm: "Base", untracked_files: str = "all") -> GitInfo:
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
    # NOTE: order is important here.
    return GitInfo(
        staged=staged,
        unstaged=unstaged,
        untracked=untracked,
        is_empty=empty_repo,
        # TODO: fix is_dirty when untracked_files="no"
        is_dirty=any([staged, unstaged, untracked]),
    )


def _diff_index_to_wtree(repo: "Repo", **kwargs: Any) -> Dict[str, List[str]]:
    from dvc_data.build import build

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
            if kwargs.get("granular", False):
                d = _granular_diff_oid(
                    root,
                    old.hash_info if old else None,
                    old,
                    new.hash_info if new else None,
                    new,
                    cache,
                    **kwargs,
                )
            else:
                d = _shallow_diff(
                    root, out.hash_info, new.hash_info if new else None, cache
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
    objs: Dict[str, Dict[str, "HashFile"]] = defaultdict(dict)
    staged_diff = defaultdict(list)
    for rev in repo.brancher(revs=[head]):
        for out in repo.index.outs:
            out = cast("Output", out)
            if not out.use_cache:
                continue

            root = str(out)
            typ = "index" if rev == "workspace" else head
            objs[root][typ] = (out.get_obj(), out.hash_info)

    cache = repo.odb.repo
    for root, obj_d in objs.items():
        old_obj, old_oid = obj_d.get(head, (None, None))
        new_obj, new_oid = obj_d.get("index", (None, None))
        with ui.status(f"Calculating diff for {root} between head/index"):
            if kwargs.get("granular", False):
                d = _granular_diff_oid(
                    root, old_oid, old_obj, new_oid, new_obj, cache, **kwargs
                )
            else:
                d = _shallow_diff(root, old_oid, new_oid, cache)

        for state, items in d.items():
            if not items:
                continue
            staged_diff[state].extend(items)

    return staged_diff


class Status(TypedDict):
    not_in_cache: List[str]
    committed: Dict[str, Any]
    uncommitted: Dict[str, Any]
    untracked: List[str]
    unchanged: List[str]
    git: GitInfo


def _transform_git_paths_to_dvc(repo: "Repo", files: List[str]):
    """Transform files rel. to Git root to DVC root, and drop outside files."""
    rel = repo.fs.path.relpath(repo.root_dir, repo.scm.root_dir).rstrip("/")
    if rel in (os.curdir, ""):
        return files

    prefix = rel + os.sep
    length = len(prefix)
    return [file[length:] for file in files if file.startswith(prefix)]


def status(repo: "Repo", untracked_files: str = "no", **kwargs: Any) -> Status:
    from scmrepo.exceptions import SCMError

    from dvc.scm import NoSCMError

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
