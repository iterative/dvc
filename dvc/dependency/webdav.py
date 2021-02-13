from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..fs.webdav import WebDAVFileSystem


class WebDAVDependency(BaseDependency, BaseOutput):
    FS_CLS = WebDAVFileSystem
