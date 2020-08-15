import logging
import os
import threading
from collections import deque

from funcy import cached_property, wrap_prop

from dvc.config import ConfigError
from dvc.exceptions import DvcException
from dvc.path_info import HTTPURLInfo, WebDAVURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree
from .http import ask_password

logger = logging.getLogger(__name__)


class WebDAVConnectionError(DvcException):
    def __init__(self, host):
        super().__init__(f"Unable to connect to WebDAV {host}.")


class WebDAVTree(BaseTree):  # pylint:disable=abstract-method
    # Use webdav scheme
    scheme = Schemes.WEBDAV

    # URLInfo for Webdav ~ replaces webdav -> http
    PATH_CLS = WebDAVURLInfo

    # Traversable as walk_files is implemented
    CAN_TRAVERSE = True

    # Length of walk_files prefix
    TRAVERSE_PREFIX_LEN = 2

    # Implementation based on webdav3.client
    REQUIRES = {"webdavclient3": "webdav3.client"}

    # Chunk size for buffered upload/download with progress bar
    CHUNK_SIZE = 2 ** 16

    # Constructor
    def __init__(self, repo, config):
        # Call BaseTree constructor
        super().__init__(repo, config)

        # Get username from configuration
        self.user = config.get("user", None)

        # Get password from configuration (might be None ~ not set)
        self.password = config.get("password", None)

        # Whether to ask for password if it is not set
        self.ask_password = config.get("ask_password", False)

        # Use token for webdav auth
        self.token = config.get("token", None)

        # Path to certificate
        self.cert_path = config.get("cert_path", None)

        # Path to private key
        self.key_path = config.get("key_path", None)

        # Connection timeout
        self.timeout = config.get("timeout", 30)

        # Get URL from configuration
        self.url = config.get("url", None)

        # If URL in config parse path_info
        if self.url:
            self.path_info = self.PATH_CLS(self.url)

            # If username not specified try to use from URL
            if self.user is None and self.path_info.user is not None:
                self.user = self.path_info.user

            # If username specified add to path_info
            if self.user is not None:
                self.path_info.user = self.user
        else:
            self.path_info = None

    # Webdav client
    @wrap_prop(threading.Lock())
    @cached_property
    def _client(self):
        from webdav3.client import Client

        # Construct hostname from path_info by stripping path
        http_info = HTTPURLInfo(self.path_info.url)
        hostname = http_info.replace(path="").url

        # Set password or ask for it
        if self.ask_password and self.password is None and self.token is None:
            host, user = self.path_info.host, self.path_info.user
            self.password = ask_password(host, user)

        # Setup webdav client options dictionary
        options = {
            "webdav_hostname": hostname,
            "webdav_login": self.user,
            "webdav_password": self.password,
            "webdav_token": self.token,
            "webdav_cert_path": self.cert_path,
            "webdav_key_path": self.key_path,
            "webdav_timeout": self.timeout,
            "webdav_chunk_size": self.CHUNK_SIZE,
        }

        client = Client(options)

        # Check whether client options are valid
        if not client.valid():
            raise ConfigError(
                f"Configuration for WebDAV {hostname} is invalid."
            )

        # Check whether connection is valid (root should always exist)
        if not client.check(self.path_info.path):
            raise WebDAVConnectionError(hostname)

        return client

    # Checks whether file/directory exists at remote
    def exists(self, path_info, use_dvcignore=True):
        # Use webdav check to test for file existence
        return self._client.check(path_info.path)

    # Gets file hash 'etag'
    def get_file_hash(self, path_info):
        # Use webdav client info method to get etag
        etag = self._client.info(path_info.path)["etag"].strip('"')

        # From HTTPTree
        if not etag:
            raise DvcException(
                "could not find an ETag or "
                "Content-MD5 header for '{url}'".format(url=path_info.url)
            )

        return self.PARAM_CHECKSUM, etag

    # Checks whether path points to directory
    def isdir(self, path_info):
        # Use webdav is_dir to test whether path points to a directory
        return self._client.is_dir(path_info.path)

    # Yields path info to all files
    def walk_files(self, path_info, **kwargs):
        # Check whether directory exists
        if not self.exists(path_info):
            return

        # Collect directories
        dirs = deque([path_info.path])

        # Iterate all directories found so far
        while dirs:
            # Iterate directory content
            for entry in self._client.list(dirs.pop(), get_info=True):
                # Construct path_info to entry
                info = path_info.replace(path=entry["path"])

                # Check whether entry is a directory
                if entry["isdir"]:
                    # Append new found directory to directory list
                    dirs.append(info.path)
                else:
                    # Yield path info to non directory
                    yield info

    # Removes file/directory
    def remove(self, path_info):
        # Use webdav client clean (DELETE) method to remove file/directory
        self._client.clean(path_info.path)

    # Creates directories
    def makedirs(self, path_info):
        # Terminate recursion
        if path_info.path == self.path_info.path or self.exists(path_info):
            return

        # Recursively descent to root
        self.makedirs(path_info.parent)

        # Construct directory at current recursion depth
        self._client.mkdir(path_info.path)

    # Moves file/directory at remote
    def move(self, from_info, to_info, mode=None):
        # Webdav client move
        self._client.move(from_info.path, to_info.path)

    # Downloads file from remote to file
    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        # Progress from HTTPTree
        with open(to_file, "wb") as fd:
            with Tqdm.wrapattr(
                fd,
                "write",
                total=None if no_progress_bar else self._file_size(from_info),
                leave=False,
                desc=from_info.url if name is None else name,
                disable=no_progress_bar,
            ) as fd_wrapped:
                # Download from WebDAV via buffer
                self._client.download_from(
                    buff=fd_wrapped, remote_path=from_info.path
                )

    # Uploads file to remote
    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        # First try to create parent directories
        self.makedirs(to_info.parent)

        # Progress from HTTPTree
        def chunks():
            with open(from_file, "rb") as fd:
                with Tqdm.wrapattr(
                    fd,
                    "read",
                    total=None
                    if no_progress_bar
                    else os.path.getsize(from_file),
                    leave=False,
                    desc=to_info.url if name is None else name,
                    disable=no_progress_bar,
                ) as fd_wrapped:
                    while True:
                        chunk = fd_wrapped.read(self.CHUNK_SIZE)
                        if not chunk:
                            break
                        yield chunk

        # Upload to WebDAV via buffer
        self._client.upload_to(buff=chunks(), remote_path=to_info.path)

    # Queries size of file at remote
    def _file_size(self, path_info):
        # Get file size from info dictionary and convert to int (from str)
        return int(self._client.info(path_info.path)["size"])
