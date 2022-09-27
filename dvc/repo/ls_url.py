from dvc.exceptions import URLMissingError
from dvc.fs import get_cloud_fs


def parse_external_url(url):
    fs_cls, fs_config, fs_path = get_cloud_fs(None, url=url)
    fs = fs_cls(**fs_config)
    return fs, fs_path


def ls_url(url, recursive=False):
    fs, fs_path = parse_external_url(url)
    return _ls_url(fs, fs_path, orig_url=url, recursive=recursive)


def _ls_url(fs, fs_path, orig_url=None, recursive=False):
    if orig_url is None:
        orig_url = fs_path
    try:
        info = fs.info(fs_path)
    except FileNotFoundError as exc:
        raise URLMissingError(orig_url) from exc
    if info["type"] != "directory":
        return [{"path": info["name"], "isdir": False}]

    ret = []
    for _, dirs, files in fs.walk(fs_path, detail=True):
        if not recursive:
            files.update(dirs)

        for info in files.values():
            ls_info = {
                "path": fs.path.relpath(info["name"], fs_path),
                "isdir": info["type"] == "directory",
            }
            ret.append(ls_info)

        if not recursive:
            break

    return ret
