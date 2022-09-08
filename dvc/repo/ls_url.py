from dvc.exceptions import URLMissingError
from dvc.output import Output


def ls_url(url):
    out = Output(None, url)
    fs, fs_path = out.fs, out.fs_path

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
