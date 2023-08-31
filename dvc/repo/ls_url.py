from dvc.exceptions import URLMissingError
from dvc.fs import parse_external_url


def ls_url(url, *, fs_config=None, recursive=False):
    fs, fs_path = parse_external_url(url, config=fs_config)
    try:
        info = fs.info(fs_path)
    except FileNotFoundError as exc:
        raise URLMissingError(url) from exc
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
                "size": info.get("size"),
            }
            ret.append(ls_info)

        if not recursive:
            break

    return ret
