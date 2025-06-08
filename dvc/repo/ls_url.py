from fsspec.implementations.local import LocalFileSystem as _LocalFileSystem

from dvc.exceptions import URLMissingError
from dvc.fs import LocalFileSystem, parse_external_url


def ls_url(url, *, fs_config=None, recursive=False, maxdepth=None, config=None):
    fs, fs_path = parse_external_url(url, fs_config=fs_config, config=config)
    try:
        info = fs.info(fs_path)
    except FileNotFoundError as exc:
        raise URLMissingError(url) from exc
    if maxdepth == 0 or info["type"] != "directory":
        return [{"path": info["name"], "isdir": False}]

    if isinstance(fs, LocalFileSystem):
        # dvc's LocalFileSystem does not support maxdepth yet
        walk = _LocalFileSystem().walk
    else:
        walk = fs.walk

    ret = []
    for root, dirs, files in walk(fs_path, detail=True, maxdepth=maxdepth):
        parts = fs.relparts(root, fs_path)
        if parts == (".",):
            parts = ()
        if not recursive or (maxdepth and len(parts) >= maxdepth - 1):
            files.update(dirs)

        for info in files.values():
            ls_info = {
                "path": fs.relpath(info["name"], fs_path),
                "isdir": info["type"] == "directory",
                "size": info.get("size"),
            }
            ret.append(ls_info)

        if not recursive:
            break

    return ret
