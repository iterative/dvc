import logging
import os
import threading
import shutil

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm

from .base import BaseTree

logger = logging.getLogger(__name__)


class WebHDFSTree(BaseTree):
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

        self.token = (
            config.get("webhdfs_token")
        )
        self.user = (
            config.get("webhdfs_user")
        )
        self.alias = (
            config.get("webhdfs_alias")
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def hdfs_client(self):
        import hdfs

        logger.debug("URL: %s", self.path_info)
        logger.debug("HDFSConfig: %s", self.hdfscli_config)

        try:
            client = hdfs.config.Config(self.hdfscli_config).get(self.alias)
        except hdfs.HdfsError:
            if self.token is not None:
                client = hdfs.TokenClient(self.path_info.url, self.token)
            else:
                client = hdfs.InsecureClient(self.path_info.url, self.user)
   
        return client

    def exists(self, path_info, use_dvcignore=True):
        status = self.hdfs_client.status(path_info.path, strict=False)
        return status is not None

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            with self.hdfs_client.write(to_info.path, encoding='utf-8') as writer:
                shutil.copyfileobj(from_file, writer)

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            with self.hdfs_client.read(from_info.path, encoding='utf-8') as reader:
                shutil.copyfileobj(reader, to_file)
