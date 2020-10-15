import logging

from .base import BaseTree
from ..path_info import CloudURLInfo
from ..scheme import Schemes

logger = logging.getLogger(__name__)


class IPFSTree(BaseTree):
    scheme = Schemes.IPFS
    PATH_CLS = CloudURLInfo

    def __init__(self, repo, config):
        super().__init__(repo, config)
        logger.debug(config["url"])
        self.path_info = IPFSTree.PATH_CLS(config["url"])
        logger.debug(self.path_info)

    def exists(self, path_info, use_dvcignore=True):
        logger.debug(f"Checking if {path_info} exists on IPFS")
        return False

    def walk_files(self, path_info, **kwargs):
        """Return a generator with `PathInfo`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path_info` will be treated as a prefix
                rather than directory path.
        """
        logger.debug(f"Walking files in {path_info} (kwargs={kwargs})")
        for file_name in self._list_paths(path_info, **kwargs):
            if file_name.endswith("/"):
                continue

            yield path_info.replace(path=file_name)

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        logger.debug(f"Uploading {from_file} (to_info={to_info}, name={name})")
        return

    def _list_paths(self, path_info, **kwargs):
        return ["foo/", "foo/bar", "foo/batz"]
