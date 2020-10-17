import logging
from pathlib import Path
from typing import Optional

import ipfshttpclient

from .base import BaseTree
from ..exceptions import DvcException
from ..path_info import URLInfo
from ..scheme import Schemes

logger = logging.getLogger(__name__)

# TODO: As long as we don't get the IPFS CID, we need to fake to be able to implement the rest
TMP_IPFS_CID_MAP = {
    "/6c/9fb857427b459ebf0a363c9319d259": "QmNg6VqLGsAcgZ1bTMniA5CamqE1bCXYMHRkao4USHnqzv",
    "/75/ee30f52010c1149d1e950b33d3adf5": "QmSgm31h9vfAn6ZKXCpVuysk45eucuG2fMoTn1dBEeRUzn",
    "/51/93c4f0e82207a00e6596f679cbdb74": "Qmaz5yXazz6mjFY5575jhb9s9RVCzPY2AHyCYY2WgPmw3V",
    "/ec/b3e4644128e3b3cf72e139ba2365c1.dir": "QmP7aqxACrAnbfFimjuciAf2pEYbHe5UQjWaZkJ6qo5j8c",
}


class IPFSTree(BaseTree):
    scheme = Schemes.IPFS
    PATH_CLS = URLInfo
    REQUIRES = {"ipfshttpclient": "ipfshttpclient"}

    def __init__(self, repo, config):
        super().__init__(repo, config)
        logger.debug(config["url"])
        self.path_info = IPFSTree.PATH_CLS(config["url"])
        self._ipfs_client: Optional[ipfshttpclient.Client] = None
        try:
            self._ipfs_client = ipfshttpclient.connect(session=True)
        except ipfshttpclient.exceptions.VersionMismatch as e:
            raise DvcException(f"Unsupported IPFS daemon ({e})") from e
        except ipfshttpclient.exceptions.ConnectionError as e:
            raise DvcException(
                "Could not connect to ipfs daemon. Install ipfs on your machine and run `ipfs daemon`"
            )

    def __del__(self):
        if self._ipfs_client is not None:
            self._ipfs_client.close()

    def exists(self, path_info: PATH_CLS, use_dvcignore=True):
        logger.debug(f"Checking if {path_info} exists")
        # TODO: we need more information than the md5 path, since IPFS is only addressable via
        #  the sha256 hash of the desired file
        #  Dig deeper into https://docs.ipfs.io/concepts/content-addressing/#identifier-formats
        #  (uses sha-256, but there is some additional processing for the final Content Identifier (CID))
        ipfs_cid = TMP_IPFS_CID_MAP[path_info.path]

        # Is there a method that checks directly the existence of a pin?
        try:
            self._ipfs_client.pin.ls(ipfs_cid)
        except ipfshttpclient.exceptions.ErrorResponse:
            return False
        else:
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
        # TODO: find a way to get notified about upload process for progress bar
        #       https://github.com/encode/httpx seems to be used in the background.
        #       Maybe httpx is configurable via kwarg "params"
        ipfs_cid = self._ipfs_client.add(from_file)["Hash"]
        logger.debug(f"Stored {from_file} at ipfs://{ipfs_cid}")
        # TODO: the ipfs_cid needs to be returned and persisted by DVC

    def _download(
        self,
        from_info: PATH_CLS,
        to_file: str,
        name=None,
        no_progress_bar=False,
    ):
        logger.debug(f"Download {from_info} to {to_file}")
        # TODO: fake mapping from path to ipfs CID
        ipfs_cid = TMP_IPFS_CID_MAP[from_info.path]

        # ipfs client downloads the file to the given directory and the filename is always the CID
        # https://github.com/ipfs-shipyard/py-ipfs-http-client/issues/48
        # Workaround by saving it to the parent directory and renaming if afterwards to the DVC expected name
        to_directory = Path(to_file).parent
        # TODO: find a way to get notified about download process for progress bar
        self._ipfs_client.get(ipfs_cid, to_directory)
        (to_directory / ipfs_cid).rename(to_file)
