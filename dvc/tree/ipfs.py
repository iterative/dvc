import logging
from funcy import cached_property
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import ipfshttpclient

from .base import BaseTree
from ..config import Config
from ..exceptions import DvcException
from ..path_info import _BasePath
from ..scheme import Schemes

logger = logging.getLogger(__name__)


class IPFSPathInfo(_BasePath):

    # This is the content id of an empty directory. It will be used when the user doesn't provide a CID
    CID_EMPTY_DIRECTORY = "QmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn"

    def __init__(self, url, mfs_path=None):
        p = urlparse(url)
        self.cid = p.netloc or IPFSPathInfo.CID_EMPTY_DIRECTORY
        self.path = p.path.rstrip("/")
        # Get the name of the project directory to provide a sane default
        project_dir_name = Path(Config().dvc_dir).parent.name
        self.mfs_path = (
            mfs_path.rstrip("/") if mfs_path else f"/dvc/{project_dir_name}"
        )
        if not self.mfs_path:
            # if mfs_path was a /, it was removed by .rstrip(). It will also clutter / if it would actually be used,
            # so just disallow it
            raise DvcException(
                "You may not use / as your IPFS MFS path. "
                "Choose another with `dvc remote modify <remote_name> mfs_path <mfs_path>`"
            )
        self.scheme = p.scheme

    @cached_property
    def url(self):
        return f"{self.scheme}://{self.cid}{self.path}"

    def __div__(self, other):
        url = f"{self.scheme}://{self.cid}{self.path}/{other}"
        return IPFSPathInfo(url, self.mfs_path)

    def __str__(self):
        return self.mfs_path + self.path

    __truediv__ = __div__


class IPFSTree(BaseTree):
    scheme = Schemes.IPFS
    PATH_CLS = IPFSPathInfo
    REQUIRES = {"ipfshttpclient": "ipfshttpclient"}

    def __init__(self, repo, config):
        super().__init__(repo, config)
        self.ipfs_client: Optional[ipfshttpclient.Client] = None
        self.path_info = IPFSTree.PATH_CLS(
            config["url"], config.get("mfs_path")
        )
        try:
            # TODO: support remote IPFS daemons with credentials
            self.ipfs_client = ipfshttpclient.connect(session=True)
        except ipfshttpclient.exceptions.VersionMismatch as e:
            raise DvcException(f"Unsupported IPFS daemon ({e})") from e
        except ipfshttpclient.exceptions.ConnectionError as e:
            raise DvcException(
                "Could not connect to ipfs daemon. Install ipfs on your machine and run `ipfs daemon`"
            )

    def __del__(self):
        if self.ipfs_client is not None:
            self.ipfs_client.close()

    def exists(self, path_info: PATH_CLS, use_dvcignore=True):
        self.ipfs_client.files.mkdir(path_info.mfs_path, parents=True)
        try:
            self.ipfs_client.files.stat(
                path_info.mfs_path + path_info.path
            )
        except ipfshttpclient.exceptions.ErrorResponse as e:
            if e.args[0] != "file does not exist":
                raise e
            return False
        return True

    def walk_files(self, path_info, **kwargs):
        logger.debug(f"Walking files in {path_info} (kwargs={kwargs})")
        # TODO: walking a file path is not possible in IPFS. We could generate a directory listing with all content
        #  of our project. For example, this is a list of all xkcd comics until Comic #1862:
        #  https://ipfs.io/ipfs/QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm
        #  This would be possible to walk, but any change on any file generates a new CID. Therefore, we need to
        #  generate a new directory listing on every update and save that CID somewhere in our project. Not sure if
        #  this is still in scope of DVC.
        #
        #  Therefore, we return an empty tree for now
        return iter(())

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        mfs_path = f"{self.path_info.mfs_path}/{to_info.path}"
        with open(from_file, "rb") as f:
            # "parents" might get a kwarg in future versions of py-ipfs-http-client? If so, change the opts param here
            self.ipfs_client.files.write(
                mfs_path, f, create=True, opts={"parents": True}
            )

    def _download(
        self,
        from_info: PATH_CLS,
        to_file: str,
        name=None,
        no_progress_bar=False,
    ):
        logger.debug(f"Downloading {from_info} to {to_file}")
        with open(to_file, "wb") as f:
            f.write(
                self.ipfs_client.files.read(
                    f"{from_info.mfs_path}/{from_info.path}"
                )
            )
