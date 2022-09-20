from dvc.exceptions import URLMissingError
from dvc.fs import get_cloud_fs
from dvc.output import OutputIsStageFileError


class Output:
    def __init__(
        self,
        path,
        fs_config=None,
    ):
        fs_config = fs_config or {}
        fs_cls, _fs_config, fs_path = get_cloud_fs(None, url=path, **fs_config)
        self.fs = fs_cls(**_fs_config)
        _validate_output_path(path)
        self.fs_path = _parse_path(self.fs, fs_path)


def _parse_path(fs, fs_path):
    return fs.path.abspath(fs.path.normpath(fs_path))


def _validate_output_path(path):
    from dvc.dvcfile import is_valid_filename

    if is_valid_filename(path):
        raise OutputIsStageFileError(path)


def get_external_fs(url, fs_config=None):
    out = Output(url, fs_config)
    return out.fs, out.fs_path


def ls_url(url):
    fs, fs_path = get_external_fs(url)

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
