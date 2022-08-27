import os
from itertools import chain
from typing import TYPE_CHECKING, Optional

from dvc.exceptions import PathMissingError

if TYPE_CHECKING:
    from dvc.fs.dvc import DvcFileSystem

    from . import Repo


def ls(
    url: str,
    path: Optional[str] = None,
    rev: str = None,
    recursive: bool = None,
    dvc_only: bool = False,
    dvc_as_dir: bool = True,
    return_sizes: bool = False,
):
    """Methods for getting files and outputs for the repo.

    Args:
        url (str): the repo url
        path (str, optional): relative path into the repo
        rev (str, optional): SHA commit, branch or tag name
        recursive (bool, optional): recursively walk the repo
        dvc_only (bool, optional): show only DVC-artifacts
        dvc_as_dir (bool, optional): return .dvc files as directories in infos
        return_sizes (bool, optional): include sizes (from metadata) in infos

    Returns:
        list of `entry`

    Notes:
        `entry` is a dictionary with structure
        {
            "path": str,
            "isout": bool,
            "isdir": bool,
            "isexec": bool,
            "size": int,  # If return_sizes is True.
        }
    """
    from . import Repo

    with Repo.open(url, rev=rev, subrepos=True, uninitialized=True) as repo:
        path = path or ""

        ret = _ls(repo, path, recursive, dvc_only, dvc_as_dir, return_sizes)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(
    repo: "Repo",
    path: str,
    recursive: bool = None,
    dvc_only: bool = False,
    # FIXME: Maybe consolidate into 'for_diskusage: bool = True'
    dvc_as_dir: bool = True,
    return_sizes: bool = False,
):
    fs: "DvcFileSystem" = repo.dvcfs
    fs_path = fs.from_os_path(path)

    try:
        fs_path = fs.info(fs_path)["name"]
    except FileNotFoundError:
        raise PathMissingError(path, repo, dvc_only=dvc_only)

    infos = {}
    for root, dirs, files in fs.walk(
        fs_path,
        dvcfiles=True,
        dvc_only=dvc_only,
        dvc_as_dir=dvc_as_dir,
    ):
        entries = chain(files, dirs) if not recursive else files

        for entry in entries:
            entry_fs_path = fs.path.join(root, entry)
            relparts = fs.path.relparts(entry_fs_path, fs_path)
            name = os.path.join(*relparts)
            infos[name] = fs.info(entry_fs_path)

        if not recursive:
            break

    if not infos and fs.isfile(fs_path):
        infos[os.path.basename(path)] = fs.info(fs_path)

    ret = {}
    for name, info in infos.items():
        dvc_info = info.get("dvc_info", {})
        if fs.isdvc(info["name"], recursive=True) or not dvc_only:
            ret[name] = {
                "isout": dvc_info.get("isout", False),
                "isdir": info["type"] == "directory",
                "isexec": info.get("isexec", False),
            }
            # TODO: Unit test for return_sizes.
            if return_sizes:
                ret[name]["size"] = info.get("size", None)

    return ret
