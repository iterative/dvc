from dvc.path_info import PathInfo
from dvc.exceptions import PathMissingError


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

    with external_repo(url, rev) as repo:
        path_info = PathInfo(repo.root_dir)
        if path:
            path_info /= path

        ret = {}
        if isinstance(repo, Repo):
            ret = _ls(repo, path_info, recursive, True)

        nondvc = {}
        if not outs_only:
            nondvc = _ls(repo, path_info, recursive, False)

        ret.update(nondvc)

        if path and not ret:
            raise PathMissingError(path, repo, output_only=outs_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(repo, path_info, recursive=None, dvc=False):
    from dvc.ignore import CleanTree
    from dvc.repo.tree import DvcTree
    from dvc.scm.tree import WorkingTree

    if dvc:
        tree = DvcTree(repo)
    else:
        tree = CleanTree(WorkingTree(repo.root_dir))

    ret = {}
    try:
        for root, dirs, files in tree.walk(path_info.fspath):
            for fname in files:
                info = PathInfo(root) / fname
                path = str(info.relative_to(path_info))
                ret[path] = {
                    "isout": dvc,
                    "isdir": False,
                    "isexec": False if dvc else tree.isexec(info.fspath),
                }

            if not recursive:
                for dname in dirs:
                    info = PathInfo(root) / dname
                    path = str(info.relative_to(path_info))
                    ret[path] = {
                        "isout": tree.isdvc(info.fspath) if dvc else False,
                        "isdir": True,
                        "isexec": False if dvc else tree.isexec(info.fspath),
                    }
                break
    except NotADirectoryError:
        return {
            path_info.name: {
                "isout": dvc,
                "isdir": False,
                "isexec": False if dvc else tree.isexec(path_info.fspath),
            }
        }
    except FileNotFoundError:
        return {}

    return ret
