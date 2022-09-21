from dvc.exceptions import URLMissingError
from dvc.fs import get_cloud_fs


def parse_external_url(url):
    fs_cls, fs_config, fs_path = get_cloud_fs(None, url=url)
    fs = fs_cls(**fs_config)
    return fs, fs.path.abspath(fs.path.normpath(fs_path))


def ls_url(url):
    fs, fs_path = parse_external_url(url)
    try:
        info = fs.info(fs_path)
    except FileNotFoundError:
        raise URLMissingError(url)
    if info["type"] != "directory":
        return [{"path": info["name"], "isdir": False}]

    infos = fs.ls(fs_path, detail=True)
    ret = []
    for info in infos:
        ls_info = {
            "path": fs.path.relpath(info["name"], fs_path),
            "isdir": info["type"] == "directory",
        }
        ret.append(ls_info)
    return ret
