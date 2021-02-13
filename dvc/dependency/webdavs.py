from ..fs.webdavs import WebDAVSFileSystem
from .webdav import WebDAVDependency


class WebDAVSDependency(WebDAVDependency):
    FS_CLS = WebDAVSFileSystem
