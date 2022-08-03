import os
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypedDict, cast

from dvc.ui import ui

if TYPE_CHECKING:
    from scmrepo.base import Base

    from dvc.output import Output
    from dvc.repo import Repo
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.obj import HashFile


def _in_cache(obj: Optional["HashFile"], cache: "HashFileDB") -> bool:
    from dvc_objects.errors import ObjectFormatError

    if not obj:
        return False
    if not obj.hash_info.value:
        return False

    try:
        cache.check(obj.hash_info.value)
        return True
    except (FileNotFoundError, ObjectFormatError):
        return False


def _shallow_diff(
    root: str,
    old_obj: Optional["HashFile"],
    new_obj: Optional["HashFile"],
    cache: "HashFileDB",
) -> Dict[str, List[str]]:
    # TODO: add support for shallow diff in dvc-data
    # TODO: we may want to recursively do in_cache check
    d = {}

    from dvc_data.objects.tree import Tree

    if isinstance(new_obj, Tree):
        root = os.path.sep.join([root, ""])

    if not _in_cache(old_obj, cache):
        d["not_in_cache"] = [root]

    if old_obj is None and new_obj is None:
        return d
    if old_obj is None:
        return {"added": [root], **d}
    if new_obj is None:
        return {"deleted": [root], **d}
    if old_obj.hash_info != new_obj.hash_info:
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

    def path_join(root: str, *paths: str) -> str:
        if not isinstance(new_obj, Tree):
            return root
        return os.path.sep.join([root, *paths])

    diff_data = odiff(old_obj, new_obj, cache)
    drop_root = not with_dirs and isinstance(new_obj, Tree)

    output: Dict[str, List[str]] = defaultdict(list)
    for state in ("added", "deleted", "modified", "unchanged"):
        items = getattr(diff_data, state)
        output[state].extend(
            path_join(root, *item.new.key)
            for item in items
            if not (drop_root and item.new.key == ROOT)
        )
        # TODO: PERF: diff is checking not_in_cache for each even if we only
        # need it for the index.
        # BUG: not_in_cache file also shows up as modified in staged and
        # unstaged. We currently don't know if it is really modified.
        output["not_in_cache"].extend(
            path_join(root, *item.new.key)
            for item in items
            if not item.old.in_cache
            and not (drop_root and item.new.key == ROOT)
            and state != "added"
        )
    return output


def _diff(
    root: str,
    old_obj: Optional["HashFile"],
    new_obj: Optional["HashFile"],
    cache: "HashFileDB",
    granular: bool = False,
    with_dirs: bool = False,
) -> Dict[str, List[str]]:
    if granular:
        return _granular_diff(
            root, old_obj, new_obj, cache, with_dirs=with_dirs
        )
    return _shallow_diff(root, old_obj, new_obj, cache)


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
            d = _diff(root, old, new, cache, **kwargs)
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
            objs[root][typ] = out.get_obj()

    cache = repo.odb.repo
    for root, obj_d in objs.items():
        old = obj_d.get(head, None)
        new = obj_d.get("index", None)
        with ui.status(f"Calculating diff for {root} between head/index"):
            d = _diff(root, old, new, cache, **kwargs)
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
