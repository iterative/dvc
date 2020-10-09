from ..tree.webdavs import WebDAVSTree
from .webdav import WebDAVDependency


class WebDAVSDependency(WebDAVDependency):
    TREE_CLS = WebDAVSTree
