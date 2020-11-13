import logging

from ..config import Config
from ..exceptions import DvcException
from ..progress import Tqdm
from .base import Remote

logger = logging.getLogger(__name__)


class IPFSRemote(Remote):
    def before_transfer(self, download=False, upload=False, gc=False):
        """Make sure that the MFS is in the desired state"""
        self._update_mfs_to_latest_cid()

    def after_transfer(self, download=False, upload=False, gc=False):
        """Calculate the final CID after a successful upload

        Changing files in our local MFS means that the content ID gets changed.
        After doing any modifications, we therefore need to update .dvc/config
        so it will always point to the latest content.
        Though we get a new CID, other users won't need to download everything
        again, since the existing files and subdirectories will keep their CID.
       """
        if not (upload or gc):
            return
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

    def _update_mfs_to_latest_cid(self):
        """
        This method makes sure that the hash of our working directory in the
        MFS matches the desired CID which is defined in .dvc/config

        It should be called before executing download operations to make sure
        the content is available, and it should be called before uploading
        new files to make sure no unwanted files get published.
        """
        mfs = self.tree.path_info.mfs_path
        files_api = self.tree.ipfs_client.files
        cid = self.tree.path_info.cid

        # doesn't change anything if the path already exists, but creates an
        # empty directory if the path didn't exists, saving us from exceptions
        files_api.mkdir(mfs, parents=True)

        current_hash = files_api.stat(mfs)["Hash"]
        if current_hash != cid:
            logger.debug(f"Updating IPFS MFS path {mfs} to CID {cid}")
            with Tqdm(
                desc=f"Updating IPFS MFS at {mfs}. This may take a while. "
                f"See progress at http://127.0.0.1:5001/webui"
            ):
                # "cp" does not like overwriting files, so delete everything
                # beforehand
                # don't worry - IPFS still keeps a cache, so we won't actually
                # downloading everything again
                files_api.rm(mfs, recursive=True)
                # this single command makes IPFS download everything.
                # any chance to provide a useful progress bar?
                # https://docs.ipfs.io/reference/http/api/#api-v0-files-cp
                files_api.cp(f"/ipfs/{cid}", mfs)
