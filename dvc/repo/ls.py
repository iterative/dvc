from dvc.exceptions import PathMissingError
from dvc.path_info import PathInfo


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

    with external_repo(url, rev) as repo:
        path_info = PathInfo(repo.root_dir)
        if path:
            path_info /= path

        ret = _ls(repo, path_info, recursive, outs_only)

        if path and not ret:
            raise PathMissingError(path, repo, output_only=outs_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(repo, path_info, recursive=None, outs_only=False):

    tree = repo.repo_tree

    ret = {}
    try:
        for root, dirs, files in tree.walk(
            path_info.fspath, dvcfiles=not outs_only
        ):
            for fname in files:
                info = PathInfo(root) / fname
                path = str(info.relative_to(path_info))
                is_out = tree.isdvc(info)
                if is_out or not outs_only:
                    ret[path] = {
                        "isout": is_out,
                        "isdir": False,
                        "isexec": tree.isexec(info),
                    }

            if not recursive:
                for dname in dirs:
                    info = PathInfo(root) / dname
                    path = str(info.relative_to(path_info))
                    is_out = tree.isdvc(info)
                    if is_out or not outs_only:
                        ret[path] = {
                            "isout": is_out if recursive else False,
                            "isdir": True,
                            "isexec": tree.isexec(info),
                        }
                break
    except NotADirectoryError:
        is_out = tree.isdvc(path_info)
        if is_out or not outs_only:
            return {
                path_info.name: {
                    "isout": is_out,
                    "isdir": False,
                    "isexec": tree.isexec(path_info.fspath),
                }
            }
    except FileNotFoundError:
        return {}

    return ret
