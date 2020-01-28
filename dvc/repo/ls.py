import os

from dvc.exceptions import PathMissingError, OutputNotFoundError


@staticmethod
def ls(url, target=None, rev=None, recursive=None, outs_only=False):
    from dvc.external_repo import external_repo
    from dvc.repo import Repo
    from dvc.utils import relpath

    with external_repo(url, rev) as repo:
        target_path_info = _get_target_path_info(repo, target)
        result = []
        if isinstance(repo, Repo):
            result.extend(_ls_outs_repo(repo, target_path_info, recursive))

        if not outs_only:
            result.extend(_ls_files_repo(target_path_info, recursive))

        if target and not result:
            raise PathMissingError(target, repo, output_only=outs_only)

        def prettify(path_info):
            if path_info == target_path_info:
                return path_info.name
            return relpath(path_info, target_path_info)

        result = list(set(map(prettify, result)))
        result.sort()
    return result


def _ls_files_repo(target_path_info, recursive=None):
    from dvc.compat import fspath
    from dvc.ignore import CleanTree
    from dvc.path_info import PathInfo
    from dvc.scm.tree import WorkingTree

    if not os.path.exists(fspath(target_path_info)):
        return []

    files = []
    tree = CleanTree(WorkingTree(target_path_info))
    try:
        for dirpath, dirnames, filenames in tree.walk(target_path_info):
            files.extend(map(lambda f: PathInfo(dirpath, f), filenames))
            if not recursive:
                files.extend(map(lambda d: PathInfo(dirpath, d), dirnames))
                break
    except NotADirectoryError:
        if os.path.isfile(fspath(target_path_info)):
            return [target_path_info]

    return files


def _ls_outs_repo(repo, target_path_info, recursive=None):
    from dvc.compat import fspath
    from dvc.path_info import PathInfo

    try:
        outs = repo.find_outs_by_path(fspath(target_path_info), recursive=True)
    except OutputNotFoundError:
        return []

    if recursive:
        return [out.path_info for out in outs]

    def get_top_part(path_info):
        relpath = path_info.relpath(target_path_info)
        if relpath.parts:
            return PathInfo(target_path_info, relpath.parts[0])
        return path_info

    return list({get_top_part(out.path_info) for out in outs})


def _get_target_path_info(repo, target=None):
    from dvc.path_info import PathInfo

    if not target:
        return PathInfo(repo.root_dir)
    return PathInfo(repo.root_dir, target)
