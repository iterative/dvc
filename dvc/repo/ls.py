import os
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from dvc.fs.dvc import DVCFileSystem


def _open_repo(
    url: str,
    rev: Optional[str] = None,
    config: Union[None, dict[str, Any], str] = None,
    remote: Optional[str] = None,
    remote_config: Optional[dict] = None,
):
    from dvc.config import Config

    from . import Repo

    if config and not isinstance(config, dict):
        config_dict = Config.load_file(config)
    else:
        config_dict = None

    return Repo.open(
        url,
        rev=rev,
        subrepos=True,
        uninitialized=True,
        config=config_dict,
        remote=remote,
        remote_config=remote_config,
    )


def _adapt_info(info: dict[str, Any]) -> dict[str, Any]:
    dvc_info = info.get("dvc_info", {})
    return {
        "isout": dvc_info.get("isout", False),
        "isdir": info["type"] == "directory",
        "isexec": info.get("isexec", False),
        "size": info.get("size"),
        "md5": dvc_info.get("md5") or dvc_info.get("md5-dos2unix"),
    }


def ls(
    url: str,
    path: Optional[str] = None,
    rev: Optional[str] = None,
    recursive: Optional[bool] = None,
    dvc_only: bool = False,
    config: Union[None, dict[str, Any], str] = None,
    remote: Optional[str] = None,
    remote_config: Optional[dict] = None,
    maxdepth: Optional[int] = None,
):
    """Methods for getting files and outputs for the repo.

    Args:
        url (str): the repo url
        path (str, optional): relative path into the repo
        rev (str, optional): SHA commit, branch or tag name
        recursive (bool, optional): recursively walk the repo
        dvc_only (bool, optional): show only DVC-artifacts
        config (str, optional): path to config file
        remote (str, optional): remote name to set as a default remote in the repo
        remote_config (str, dict): remote config to merge with a remote in the repo

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
    with _open_repo(url, rev, config, remote, remote_config) as repo:
        path = path or ""
        fs: DVCFileSystem = repo.dvcfs
        fs_path = fs.from_os_path(path)
        return _ls(fs, fs_path, recursive, dvc_only, maxdepth)


def ls_tree(
    url: str,
    path: Optional[str] = None,
    rev: Optional[str] = None,
    dvc_only: bool = False,
    config: Union[None, dict[str, Any], str] = None,
    remote: Optional[str] = None,
    remote_config: Optional[dict] = None,
    maxdepth: Optional[int] = None,
):
    with _open_repo(url, rev, config, remote, remote_config) as repo:
        path = path or ""
        fs: DVCFileSystem = repo.dvcfs
        fs_path = fs.from_os_path(path)
        return _ls_tree(fs, fs_path, dvc_only, maxdepth)


def _ls(
    fs: "DVCFileSystem",
    path: str,
    recursive: Optional[bool] = None,
    dvc_only: bool = False,
    maxdepth: Optional[int] = None,
):
    fs_path = fs.info(path)["name"]

    infos = {}

    # ignore maxdepth only if recursive is not set
    maxdepth = maxdepth if recursive else None
    if maxdepth == 0 or fs.isfile(fs_path):
        infos[os.path.basename(path) or os.curdir] = fs.info(fs_path)
    else:
        for root, dirs, files in fs.walk(
            fs_path,
            dvcfiles=True,
            dvc_only=dvc_only,
            detail=True,
            maxdepth=maxdepth,
        ):
            parts = fs.relparts(root, fs_path)
            if parts == (".",):
                parts = ()
            if not recursive or (maxdepth and len(parts) >= maxdepth - 1):
                files.update(dirs)

            for name, entry in files.items():
                infos[os.path.join(*parts, name)] = entry

            if not recursive:
                break

    ret_list = []
    for p, info in sorted(infos.items(), key=lambda x: x[0]):
        _info = _adapt_info(info)
        _info["path"] = p
        ret_list.append(_info)
    return ret_list


def _ls_tree(
    fs, path, dvc_only: bool = False, maxdepth: Optional[int] = None, _info=None
):
    ret = {}
    info = _info or fs.info(path)

    path = info["name"].rstrip(fs.sep) or os.curdir
    name = path.rsplit("/", 1)[-1]
    ls_info = _adapt_info(info)
    ls_info["path"] = path

    recurse = maxdepth is None or maxdepth > 0
    if recurse and info["type"] == "directory":
        infos = fs.ls(path, dvcfiles=True, dvc_only=dvc_only, detail=True)
        infos.sort(key=lambda f: f["name"])
        maxdepth = maxdepth - 1 if maxdepth is not None else None
        contents = {}
        for info in infos:
            d = _ls_tree(
                fs, info["name"], dvc_only=dvc_only, maxdepth=maxdepth, _info=info
            )
            contents.update(d)
        ls_info["contents"] = contents

    ret[name] = ls_info
    return ret
