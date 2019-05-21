from __future__ import unicode_literals

import os
import logging

try:
    import google_auth_oauthlib
    from dvc.remote.gdrive.client import GDriveClient
except ImportError:
    google_auth_oauthlib = None

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.remote.gdrive.utils import (
    TrackFileReadProgress,
    only_once,
    metadata_isdir,
    shared_token_warning,
)
from dvc.remote.gdrive.exceptions import GDriveError, GDriveResourceNotFound


logger = logging.getLogger(__name__)


class GDriveURLInfo(CloudURLInfo):
    @property
    def netloc(self):
        return self.parsed.netloc


class RemoteGDrive(RemoteBASE):
    """Google Drive remote implementation

    ## Some notes on Google Drive design

    Google Drive differs from S3 and GS remotes - it identifies the resources
    by IDs instead of paths.

    Folders are regular resources with an `application/vnd.google-apps.folder`
    MIME type. Resource can have multiple parent folders, and also there could
    be multiple resources with the same name linked to a single folder, so
    files could be duplicated.

    There are multiple root folders accessible from a single user account:
    - `root` (special ID) - alias for the "My Drive" folder
    - `appDataFolder` (special ID) - alias for the hidden application
    space root folder
    - shared drives root folders

    ## Example URLs

    - Datasets/my-dataset inside "My Drive" folder:

        gdrive://root/Datasets/my-dataset

    - Folder by ID (recommended):

        gdrive://1r3UbnmS5B4-7YZPZmyqJuCxLVps1mASC

        (get it https://drive.google.com/drive/folders/{here})

    - Dataset named "my-dataset" in the hidden application folder:

        gdrive://appDataFolder/my-dataset

        (this one wouldn't be visible through Google Drive web UI and
         couldn't be shared)
    """

    scheme = Schemes.GDRIVE
    path_cls = GDriveURLInfo
    REGEX = r"^gdrive://.*$"
    REQUIRES = {"google-auth-oauthlib": google_auth_oauthlib}
    PARAM_CHECKSUM = "md5Checksum"
    SPACE_DRIVE = "drive"
    SCOPE_DRIVE = "https://www.googleapis.com/auth/drive"
    SPACE_APPDATA = "appDataFolder"
    SCOPE_APPDATA = "https://www.googleapis.com/auth/drive.appdata"
    DEFAULT_OAUTH_ID = "default"

    # Default credential is needed to show the string of "Data Version
    # Control" in OAuth dialog application name and icon in authorized
    # applications list in Google account security settings. Also, the
    # quota usage is limited by the application defined by client_id.
    # The good practice would be to suggest the user to create their
    # own application credentials.
    DEFAULT_CREDENTIALPATH = os.path.join(
        os.path.dirname(__file__), "google-dvc-client-id.json"
    )

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.root = self.path_info.netloc.lower()
        if self.root == self.SPACE_APPDATA.lower():
            default_scopes = self.SCOPE_APPDATA
            space = self.SPACE_APPDATA
        else:
            default_scopes = self.SCOPE_DRIVE
            space = self.SPACE_DRIVE
        if Config.SECTION_GDRIVE_CREDENTIALPATH not in config:
            shared_token_warning()
            credentialpath = config.get(
                Config.SECTION_GDRIVE_CREDENTIALPATH,
                self.DEFAULT_CREDENTIALPATH,
            )
        scopes = config.get(Config.SECTION_GDRIVE_SCOPES, default_scopes)
        # scopes should be a list and it is space-delimited in all
        # configs, and `.split()` also works for a single-element list
        scopes = scopes.split()

        core_config = self.repo.config.config[Config.SECTION_CORE]
        oauth2_flow_runner = core_config.get(
            Config.SECTION_CORE_OAUTH2_FLOW_RUNNER, "console"
        )

        self.client = GDriveClient(
            space,
            config.get(Config.SECTION_GDRIVE_OAUTH_ID, self.DEFAULT_OAUTH_ID),
            credentialpath,
            scopes,
            oauth2_flow_runner,
        )

    def get_file_checksum(self, path_info):
        metadata = self.client.get_metadata(path_info, fields=["md5Checksum"])
        return metadata["md5Checksum"]

    def exists(self, path_info):
        return self.client.exists(path_info)

    def batch_exists(self, path_infos, callback):
        results = []
        for path_info in path_infos:
            results.append(self.exists(path_info))
            callback.update(str(path_info))
        return results

    def list_cache_paths(self):
        try:
            root = self.client.get_metadata(self.path_info)
        except GDriveResourceNotFound as e:
            logger.debug("list_cache_paths: {}".format(e))
        else:
            prefix = self.path_info.path
            for i in self.client.list_children(root["id"]):
                yield prefix + "/" + i

    @only_once
    def mkdir(self, parent, name):
        return self.client.mkdir(parent, name)

    def makedirs(self, path_info):
        parent = path_info.netloc
        parts = iter(path_info.path.split("/"))
        current_path = ["gdrive://" + path_info.netloc]
        for part in parts:
            try:
                metadata = self.client.get_metadata(
                    self.path_cls.from_parts(
                        self.scheme, parent, path="/" + part
                    )
                )
            except GDriveResourceNotFound:
                break
            else:
                current_path.append(part)
                if not metadata_isdir(metadata):
                    raise GDriveError(
                        "{} is not a folder".format("/".join(current_path))
                    )
                parent = metadata["id"]
        to_create = [part] + list(parts)
        for part in to_create:
            parent = self.mkdir(parent, part)["id"]
        return parent

    def _upload(self, from_file, to_info, name, no_progress_bar):

        dirname = to_info.parent.path
        if dirname:
            try:
                parent = self.client.get_metadata(to_info.parent)
            except GDriveResourceNotFound:
                parent = self.makedirs(to_info.parent)
        else:
            parent = to_info.netloc

        from_file = open(from_file, "rb")
        if not no_progress_bar:
            from_file = TrackFileReadProgress(name, from_file)

        try:
            self.client.upload(parent, to_info, from_file)
        finally:
            from_file.close()

    def _download(self, from_info, to_file, name, no_progress_bar):
        self.client.download(from_info, to_file, name, no_progress_bar)
