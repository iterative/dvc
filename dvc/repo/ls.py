import os
import stat

from dvc.exceptions import PathMissingError, OutputNotFoundError


@staticmethod
def ls(
    url, path=None, rev=None, recursive=None, outs_only=False,
):
    """Methods for getting files and outputs for the repo.

    Args:
        url (str): the repo url
        path (str, optional): relative path into the repo
        rev (str, optional): SHA commit, branch or tag name
        recursive (bool, optional): recursively walk the repo
        outs_only (bool, optional): show only DVC-artifacts

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
    from dvc.external_repo import external_repo
    from dvc.repo import Repo
    from dvc.utils import relpath

    with external_repo(url, rev) as repo:
        path_info = _get_path_info(repo, path)
        fs_nodes = []
        if isinstance(repo, Repo):
            fs_nodes.extend(_ls_outs_repo(repo, path_info, recursive))

        if not outs_only:
            fs_nodes.extend(_ls_files_repo(path_info, recursive))

        if path and not fs_nodes:
            raise PathMissingError(path, repo, output_only=outs_only)

        fs_nodes = {n["path_info"]: n for n in fs_nodes}.values()

        def get_entry(fs_node):
            node_path_info = fs_node["path_info"]
            path = (
                node_path_info.name
                if node_path_info == path_info
                else relpath(node_path_info, path_info)
            )
            return {
                "path": path,
                "isout": fs_node.get("isout", False),
                "isdir": fs_node.get("isdir", False),
                "isexec": fs_node.get("isexec", False),
            }

        entries = sorted(map(get_entry, fs_nodes), key=lambda f: f["path"])
    return entries


def _ls_files_repo(path_info, recursive=None):
    from dvc.compat import fspath
    from dvc.ignore import CleanTree
    from dvc.path_info import PathInfo
    from dvc.scm.tree import WorkingTree

    if not os.path.exists(fspath(path_info)):
        return []

    files = []
    tree = CleanTree(WorkingTree(path_info))
    try:
        for dirpath, dirnames, filenames in tree.walk(path_info):
            files.extend(PathInfo(dirpath, f) for f in filenames)
            if not recursive:
                files.extend(PathInfo(dirpath, d) for d in dirnames)
                break
    except NotADirectoryError:
        if os.path.isfile(fspath(path_info)):
            files = [path_info]

    return [_get_fs_node(f) for f in files]


def _ls_outs_repo(repo, path_info, recursive=None):
    from dvc.compat import fspath
    from dvc.path_info import PathInfo

    try:
        outs = repo.find_outs_by_path(fspath(path_info), recursive=True)
    except OutputNotFoundError:
        return []

    if recursive:
        return [_get_fs_node(out.path_info, out) for out in outs]

    def get_first_segment(out):
        """Returns tuple with path_info and related out

        path_info calculated as the first relpath segment
        Example:
            dir/file -> dir
            dir/subdir/file -> dir
            file -> file
        """
        relpath = out.path_info.relpath(path_info)
        if relpath.parts:
            out_path_info = PathInfo(path_info, relpath.parts[0])
            isout = len(relpath.parts) == 1
            return (out_path_info, out if isout else None)
        return (out.path_info, out)

    return [
        _get_fs_node(p, out)
        for (p, out) in {get_first_segment(out) for out in outs}
    ]


def _get_path_info(repo, path=None):
    from dvc.path_info import PathInfo

    if not path:
        return PathInfo(repo.root_dir)
    return PathInfo(repo.root_dir, path)


def _get_fs_node(path_info, out=None):
    from dvc.compat import fspath

    if out:
        isdir = out.is_dir_checksum if out.checksum else False
        isexec = False
    else:
        try:
            isdir = os.path.isdir(fspath(path_info))
            mode = os.stat(fspath(path_info)).st_mode
            isexec = mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        except FileNotFoundError:
            isdir = False
            isexec = False

    return {
        "path_info": path_info,
        "isout": bool(out),
        "isdir": isdir,
        "isexec": isexec,
    }
