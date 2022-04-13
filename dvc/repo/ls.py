import os
from itertools import chain

from dvc.exceptions import PathMissingError


def ls(url, path=None, rev=None, recursive=None, dvc_only=False):
    """Methods for getting files and outputs for the repo.

    Args:
        url (str): the repo url
        path (str, optional): relative path into the repo
        rev (str, optional): SHA commit, branch or tag name
        recursive (bool, optional): recursively walk the repo
        dvc_only (bool, optional): show only DVC-artifacts

    Returns:
        list of `entry`

    Notes:
        `entry` is a dictionary with structure
        {
            "path": str,
            "isout": bool,
            "isdir": bool,
            "isexec": bool,
        }
    """
    from . import Repo

    with Repo.open(url, rev=rev, subrepos=True, uninitialized=True) as repo:
        path = path or ""

        ret = _ls(repo.repo_fs, path, recursive, dvc_only)

        if path and not ret:
            raise PathMissingError(path, repo, dvc_only=dvc_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(fs, path, recursive=None, dvc_only=False):
    try:
        fs_path = fs.info(path)["name"]
    except FileNotFoundError:
        return {}

    infos = {}
    for root, dirs, files in fs.walk(fs_path, dvcfiles=True):
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
        if dvc_info.get("outs") or not dvc_only:
            ret[name] = {
                "isout": dvc_info.get("isout", False),
                "isdir": info["type"] == "directory",
                "isexec": info.get("isexec", False),
            }

    return ret
