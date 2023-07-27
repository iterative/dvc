import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dvc.fs.dvc import DVCFileSystem

    from . import Repo


def ls(
    url: str,
    path: Optional[str] = None,
    rev: Optional[str] = None,
    recursive: Optional[bool] = None,
    dvc_only: bool = False,
    config: Optional[str] = None,
    remote: Optional[str] = None,
    remote_config: Optional[dict] = None,
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
    from dvc.config import Config

    from . import Repo

    if config and not isinstance(config, dict):
        config_dict = Config.load_file(config)
    else:
        config_dict = None

    with Repo.open(
        url,
        rev=rev,
        subrepos=True,
        uninitialized=True,
        config=config_dict,
        remote=remote,
        remote_config=remote_config,
    ) as repo:
        path = path or ""

        ret = _ls(repo, path, recursive, dvc_only)

        ret_list = []
        for path, info in ret.items():
            info["path"] = path
            ret_list.append(info)
        ret_list.sort(key=lambda f: f["path"])
        return ret_list


def _ls(
    repo: "Repo",
    path: str,
    recursive: Optional[bool] = None,
    dvc_only: bool = False,
):
    fs: "DVCFileSystem" = repo.dvcfs
    fs_path = fs.from_os_path(path)

    fs_path = fs.info(fs_path)["name"]

    infos = {}
    for root, dirs, files in fs.walk(
        fs_path, dvcfiles=True, dvc_only=dvc_only, detail=True
    ):
        if not recursive:
            files.update(dirs)

        parts = fs.path.relparts(root, fs_path)
        if parts == (".",):
            parts = ()

        for name, entry in files.items():
            infos[os.path.join(*parts, name)] = entry

        if not recursive:
            break

    if not infos and fs.isfile(fs_path):
        infos[os.path.basename(path)] = fs.info(fs_path)

    ret = {}
    for name, info in infos.items():
        dvc_info = info.get("dvc_info", {})
        ret[name] = {
            "isout": dvc_info.get("isout", False),
            "isdir": info["type"] == "directory",
            "isexec": info.get("isexec", False),
        }

    return ret
