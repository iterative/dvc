import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


class WebHDFSTree(BaseTree):
    scheme = Schemes.WEBHDFS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"hdfs": "hdfs"}

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        self.path_info = self.PATH_CLS(url) if url else None

        self.hdfscli_config = (
            config.get("hdfscli_config")
            or os.getenv("HDFSCLI_CONFIG")
            or "~/.hdfscli.cfg"
        )

        self.token = config.get("webhdfs_token")
        self.user = config.get("user")
        self.alias = config.get("webhdfs_alias")

    @wrap_prop(threading.Lock())
    @cached_property
    def hdfs_client(self):
        import hdfs

        logger.debug("URL: %s", self.path_info)
        logger.debug("HDFSConfig: %s", self.hdfscli_config)

        try:
            client = hdfs.config.Config(  # pylint: disable=no-member
                self.hdfscli_config
            ).get(self.alias)
        except hdfs.util.HdfsError:  # pylint: disable=no-member
            if self.token is not None:
                client = hdfs.TokenClient(  # pylint: disable=no-member
                    self.path_info.url, self.token
                )
            else:
                client = hdfs.InsecureClient(  # pylint: disable=no-member
                    self.path_info.url, self.user
                )

        return client

    def walk_files(self, path_info, **kwargs):
        yield self.hdfs_client.walk(
            path_info.path, depth=kwargs.get("depth", 0)
        )

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        self.hdfs_client.delete(path_info.path)

    def exists(self, path_info, use_dvcignore=True):
        status = self.hdfs_client.status(path_info.path, strict=False)
        return status is not None

    def get_file_hash(self, path_info):
        return self.hdfs_client.checksum(path_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.hdfs_client.upload(to_info.path, from_file, progress=pbar)

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.hdfs_client.download(from_info.path, to_file, progress=pbar)
