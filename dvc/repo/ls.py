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

        ret = _ls(repo.repo_fs, fs_path, recursive, dvc_only)

        if path and not ret:
            raise PathMissingError(path, repo, dvc_only=dvc_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(fs, fs_path, recursive=None, dvc_only=False):
    def onerror(exc):
        raise exc

    infos = []
    try:
        for root, dirs, files in fs.walk(
            fs_path, onerror=onerror, dvcfiles=True
        ):
            entries = chain(files, dirs) if not recursive else files
            infos.extend(fs.path.join(root, entry) for entry in entries)
            if not recursive:
                break
    except NotADirectoryError:
        infos.append(fs_path)
    except FileNotFoundError:
        return {}

    ret = {}
    for info in infos:
        metadata = fs.metadata(info)
        if metadata.output_exists or not dvc_only:
            path = (
                fs.path.name(fs_path)
                if fs_path == info
                else fs.path.relpath(info, fs_path)
            )
            ret[path] = {
                "isout": metadata.is_output,
                "isdir": metadata.isdir,
                "isexec": metadata.is_exec,
            }
    return ret
