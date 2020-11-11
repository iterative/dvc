import logging

from .base import Remote
from ..config import Config
from ..exceptions import DvcException

logger = logging.getLogger(__name__)


class IPFSRemote(Remote):
    def after_upload(self):
        """Calculate the final CID after a successful upload

        Changing files in our local MFS means that the content ID gets changed. After doing any modifications,
        we therefore need to update .dvc/config so it will always point to the latest content.
        Though we get a new CID, other users won't need to download everything again, since the existing files
        and subdirectories will keep their CID.
        """
        path_info = self.tree.path_info
        old_cid = path_info.cid
        new_cid = self.tree.ipfs_client.files.stat(path_info.mfs_path)["Hash"]
        logger.debug(f"Saving new CID ipfs://{new_cid}")
        with Config().edit("repo") as repo_config:
            section = None
            for v in repo_config["remote"].values():
                url = v.get("url")
                if url == "ipfs://":
                    url = f"ipfs://{path_info.CID_EMPTY_DIRECTORY}"
                if url == "ipfs://" + old_cid:
                    section = v
                    break
            if not section:
                raise DvcException("Could not find ipfs config in .dvc/config")
            section["url"] = "ipfs://" + new_cid
            path_info.cid = new_cid
