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
        fs_path = repo.root_dir
        if path:
            fs_path = os.path.abspath(repo.fs.path.join(fs_path, path))

        ret = _ls(repo, fs_path, recursive, dvc_only)

        if path and not ret:
            raise PathMissingError(path, repo, dvc_only=dvc_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(repo, fs_path, recursive=None, dvc_only=False):
    fs = repo.repo_fs
    infos = []
    for root, dirs, files in fs.walk(fs_path, dvcfiles=True):
        entries = chain(files, dirs) if not recursive else files
        infos.extend(fs.path.join(root, entry) for entry in entries)
        if not recursive:
            break

    if not infos and fs.isfile(fs_path):
        infos.append(fs_path)

    ret = {}
    for info in infos:
        try:
            _info = fs.info(info)
        except FileNotFoundError:
            # broken symlink
            _info = {"type": "file", "isexec": False}

        if _info.get("outs") or not dvc_only:
            path = (
                fs.path.name(fs_path)
                if fs_path == info
                else fs.path.relpath(info, fs_path)
            )
            ret[path] = {
                "isout": _info.get("isout", False),
                "isdir": _info["type"] == "directory",
                "isexec": _info["isexec"],
            }
    return ret
