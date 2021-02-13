from ..fs import get_cloud_fs
from .base import Remote
from .local import LocalRemote


def get_remote(repo, **kwargs):
    fs = get_cloud_fs(repo, **kwargs)
    if fs.scheme == "local":
        return LocalRemote(fs)
    return Remote(fs)
