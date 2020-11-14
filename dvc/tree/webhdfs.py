import logging
import os
import threading
from contextlib import contextmanager
from urllib.parse import urlparse

from funcy import cached_property, wrap_prop

from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


def update_pbar(pbar, total):
    """Update pbar to accept the two arguments passed by hdfs"""

    def update(_, bytes_transfered):
        if bytes_transfered == -1:
            pbar.update_to(total)
            return
        pbar.update_to(bytes_transfered)

    return update


class WebHDFSTree(BaseTree):
    scheme = Schemes.WEBHDFS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"hdfs": "hdfs"}
    PARAM_CHECKSUM = "checksum"

    def __init__(self, repo, config):
        super().__init__(repo, config)

        self.path_info = None
        url = config.get("url")
        if not url:
            return

        parsed = urlparse(url)
        user = parsed.username or config.get("user")

        self.path_info = self.PATH_CLS.from_parts(
            scheme="webhdfs",
            host=parsed.hostname,
            user=user,
            port=parsed.port,
            path=parsed.path,
        )

        self.hdfscli_config = config.get("hdfscli_config")
        self.token = config.get("webhdfs_token")
        self.alias = config.get("webhdfs_alias")

    @wrap_prop(threading.Lock())
    @cached_property
    def hdfs_client(self):
        import hdfs

        logger.debug("URL: %s", self.path_info)
        logger.debug("HDFSConfig: %s", self.hdfscli_config)

        try:
            return hdfs.config.Config(self.hdfscli_config).get_client(
                self.alias
            )
        except hdfs.util.HdfsError as exc:
            exc_msg = str(exc)
            errors = (
                "No alias specified",
                "Invalid configuration file",
                f"Alias {self.alias} not found",
            )
            if not any(err in exc_msg for err in errors):
                raise

            http_url = f"http://{self.path_info.host}:{self.path_info.port}"

            if self.token is not None:
                client = hdfs.TokenClient(http_url, token=self.token, root="/")
            else:
                client = hdfs.InsecureClient(
                    http_url, user=self.path_info.user, root="/"
                )

        return client

    @contextmanager
    def open(self, path_info, mode="r", encoding=None):
        assert mode in {"r", "rt", "rb"}

        with self.hdfs_client.read(
            path_info.path, encoding=encoding
        ) as reader:
            yield reader.read()

    def walk_files(self, path_info, **kwargs):
        if not self.exists(path_info):
            return

        root = path_info.path
        for path, _, files in self.hdfs_client.walk(root):
            for file_ in files:
                path = os.path.join(path, file_)
                yield path_info.replace(path=path)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        self.hdfs_client.delete(path_info.path)

    def exists(self, path_info, use_dvcignore=True):
        assert not isinstance(path_info, list)
        assert path_info.scheme == "webhdfs"

        status = self.hdfs_client.status(path_info.path, strict=False)
        return status is not None

    def get_file_hash(self, path_info):
        checksum = self.hdfs_client.checksum(path_info.path)
        hash_info = HashInfo(self.PARAM_CHECKSUM, checksum["bytes"])

        hash_info.size = self.hdfs_client.status(path_info.path)["length"]
        return hash_info

    def copy(self, from_info, to_info, **_kwargs):
        with self.hdfs_client.read(from_info.path) as reader:
            content = reader.read()
        self.hdfs_client.write(to_info.path, data=content)

    def move(self, from_info, to_info, mode=None):
        self.hdfs_client.makedirs(to_info.parent.path)
        self.hdfs_client.rename(from_info.path, to_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        total = os.path.getsize(from_file)
        with Tqdm(
            desc=name, total=total, disable=no_progress_bar, bytes=True
        ) as pbar:
            self.hdfs_client.upload(
                to_info.path,
                from_file,
                overwrite=True,
                progress=update_pbar(pbar, total),
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        total = self.hdfs_client.status(from_info.path)["length"]
        with Tqdm(
            desc=name, total=total, disable=no_progress_bar, bytes=True
        ) as pbar:
            self.hdfs_client.download(
                from_info.path, to_file, progress=update_pbar(pbar, total)
            )
