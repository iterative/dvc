import logging
import os

from dvc.dvcfile import is_dvc_file

logger = logging.getLogger(__name__)


def _get_file_size(fs, path):
    """Method to get size of files tracked by git and dvc

    Args:
        fs (RepoFileSystem): repo fs object
        path (str): absolute path as given by the repo open API

    Returns:
        size in integer and 0 in case of error
    """
    size = 0
    if not is_dvc_file(path):
        return fs.getsize(path)
    try:
        metadata = fs.metadata(path.split(".")[0])
        if len(metadata.outs) > 0:
            size = metadata.outs[0].meta.size
    except:
        # TODO: handle specific error and report
        return size
    return size


def _get_path_size(fs, path, depth, ps, include_files=False):
    """Method to calculate total size of a path, File or Directory
    Tracked by DVC or Git

    Args:
        fs (RepoFileSystem): repo fs object
        path (str): absolute path as given by the repo open API
        depth (int): current recursive depth alllowed
        ps (dict): path to size map
        include_files (bool, Optional): enable or disable files in ps map

    Returns:
        size in integer and 0 in case of error
    """
    total = 0

    def onerror(exc):
        raise exc

    try:
        root, dirs, files = next(fs.walk(path, onerror=onerror, dvcfiles=True))
        for f in files:
            file_path = fs.path.join(root, f)
            sz = _get_file_size(fs, file_path)
            total += sz
            if include_files:
                ps[file_path] = sz
        for d in dirs:
            dir_path = fs.path.join(root, d)
            ps[dir_path] = _get_path_size(fs, dir_path, depth - 1, ps)
            total += ps[dir_path]
        if depth == 0:
            return total
    except NotADirectoryError:
        if include_files:
            ps[path] = _get_file_size(fs, path)
            total += ps[path]
    except FileNotFoundError:
        raise FileNotFoundError()
    return total


def du(
    url,
    path=None,
    rev=None,
    summarize=False,
    max_depth=-1,
    include_files=False,
):
    """Method for getting dist usage for the repo

    Args:
        url (str): the repo url
        path (str, optional): relative path into the repo
        rev (str, optional): SHA commit, branch or tag name
        summarize (bool, optional): enable or disable summarized output
        max_depth (int, optional): max depth to recursively list files and dirs
        include_files (bool, optional): enable or disable file entries

    Returns:
        list of `entry`, total_size

    Notes:
        `entry` is a dictionary with path as key and structure
        {
            "size": int
        }
    """
    from . import Repo

    with Repo.open(url, rev=rev, subrepos=True, uninitialized=True) as repo:
        fs_path = repo.root_dir
        if path:
            fs_path = os.path.abspath(repo.fs.path.join(fs_path, path))

        fs = repo.repo_fs
        if summarize:
            max_depth = 0

        path_size = {}
        try:
            path_size[fs_path] = _get_path_size(
                fs, fs_path, max_depth, path_size, include_files=include_files
            )
        except FileNotFoundError:
            logger.exception("File not found error")
            return []

        ret_list = []
        for path, size in path_size.items():
            fixed_path = path.replace(repo.root_dir, "")
            if is_dvc_file(path):
                # Adding annotation to identify DVC tracked Data in ui output
                fixed_path = (
                    "".join(fixed_path.split(".")[:-1])
                    + " [*] DVC Tracked Data"
                )
            if size:
                ret_list.append({"path": fixed_path, "size": size})
        return ret_list, path_size[fs_path]
