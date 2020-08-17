from dvc.exceptions import PathMissingError
from dvc.path_info import PathInfo


@staticmethod
def ls(
    url, path=None, rev=None, recursive=None, dvc_only=False,
):
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
    from dvc.external_repo import external_repo

    with external_repo(url, rev) as repo:
        path_info = PathInfo(repo.root_dir)
        if path:
            path_info /= path

        ret = _ls(repo, path_info, recursive, dvc_only)

        if path and not ret:
            raise PathMissingError(path, repo, dvc_only=dvc_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(repo, path_info, recursive=None, dvc_only=False):
    from dvc.repo.tree import RepoTree

    def onerror(exc):
        raise exc

    # use our own RepoTree instance instead of repo.repo_tree since we want to
    # fetch directory listings, but don't want to fetch file contents.
    tree = RepoTree(repo, stream=True)

    ret = {}
    try:
        for root, dirs, files in tree.walk(
            path_info.fspath, onerror=onerror, dvcfiles=True
        ):
            for fname in files:
                info = PathInfo(root) / fname
                dvc = tree.isdvc(info)
                if dvc or not dvc_only:
                    path = str(info.relative_to(path_info))
                    ret[path] = {
                        "isout": dvc,
                        "isdir": False,
                        "isexec": False if dvc else tree.isexec(info),
                    }

            if not recursive:
                for dname in dirs:
                    info = PathInfo(root) / dname
                    # pylint:disable=protected-access
                    _, dvctree = tree._get_tree_pair(info)  # noqa
                    if not dvc_only or (dvctree and dvctree.exists(info)):
                        dvc = tree.isdvc(info)
                        path = str(info.relative_to(path_info))
                        ret[path] = {
                            "isout": dvc,
                            "isdir": True,
                            "isexec": False if dvc else tree.isexec(info),
                        }
                break
    except NotADirectoryError:
        dvc = tree.isdvc(path_info)
        if dvc or not dvc_only:
            return {
                path_info.name: {
                    "isout": dvc,
                    "isdir": False,
                    "isexec": False if dvc else tree.isexec(path_info),
                }
            }
        return {}
    except FileNotFoundError:
        return {}

    return ret
