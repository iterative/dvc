from __future__ import unicode_literals

import os
import posixpath
import logging

from funcy import cached_property, retry

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.remote.gdrive.pydrive import (
    RequestListFile,
    RequestListFilePaginated,
    RequestCreateFolder,
    RequestUploadFile,
    RequestDownloadFile,
)
from dvc.remote.gdrive.utils import FOLDER_MIME_TYPE

logger = logging.getLogger(__name__)

class GDriveRetriableError(DvcException):
    def __init__(self, msg):
        super(GDriveRetriableError, self).__init__(msg)

class RemoteGDrive(RemoteBASE):
    scheme = Schemes.GDRIVE
    path_cls = CloudURLInfo
    REGEX = r"^gdrive://.*$"
    REQUIRES = {"pydrive": "pydrive"}
    GDRIVE_USER_CREDENTIALS_DATA = "GDRIVE_USER_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = ".dvc/tmp/gdrive-user-credentials.json"

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.no_traverse = False
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.config = config
        self.init_drive()

    def init_drive(self):
        self.gdrive_client_id = self.config.get(
            Config.SECTION_GDRIVE_CLIENT_ID, None
        )
        self.gdrive_client_secret = self.config.get(
            Config.SECTION_GDRIVE_CLIENT_SECRET, None
        )
        if not self.gdrive_client_id or not self.gdrive_client_secret:
            raise DvcException(
                "Please specify Google Drive's client id and "
                "secret in DVC's config. Learn more at "
                "https://man.dvc.org/remote/add."
            )
        self.gdrive_user_credentials_path = self.config.get(
            Config.SECTION_GDRIVE_USER_CREDENTIALS_FILE,
            self.DEFAULT_USER_CREDENTIALS_FILE,
        )

        self.root_id = self.get_path_id(self.path_info, create=True)
        self.cached_dirs, self.cached_ids = self.cache_root_dirs()

    # 8 tries, start at 0.5s, multiply by golden ratio, cap at 10s
    @retry(8,
        errors=(GDriveRetriableError),
        timeout=lambda a: min(0.5 * 1.618 ** a, 10))
    def execute_request(self, request):
        from pydrive.files import ApiRequestError
        try:
            result = request.execute()
        except Exception as exception:
            retry_codes = ["403", "500", "502", "503", "504"]
            if any("HttpError {}".format(code) in str(exception) for code in retry_codes):
                raise GDriveRetriableError("Google API request failed")
            raise
        return result

    def list_drive_item(self, query):
        list_request = RequestListFilePaginated(self.drive, query)
        page_list = self.execute_request(list_request)
        while page_list:
            for item in page_list:
                yield item
            page_list = self.execute_request(list_request)

    def cache_root_dirs(self):
        cached_dirs = {}
        cached_ids = {}
        for dir1 in self.list_drive_item(
            "'{}' in parents and trashed=false".format(self.root_id)
        ):
            cached_dirs.setdefault(dir1["title"], []).append(dir1["id"])
            cached_ids[dir1["id"]] = dir1["title"]
        return cached_dirs, cached_ids

    @cached_property
    def drive(self):
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive

        if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA):
            with open(
                self.gdrive_user_credentials_path, "w"
            ) as credentials_file:
                credentials_file.write(
                    os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA)
                )

        GoogleAuth.DEFAULT_SETTINGS["client_config_backend"] = "settings"
        GoogleAuth.DEFAULT_SETTINGS["client_config"] = {
            "client_id": self.gdrive_client_id,
            "client_secret": self.gdrive_client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "revoke_uri": "https://oauth2.googleapis.com/revoke",
            "redirect_uri": "",
        }
        GoogleAuth.DEFAULT_SETTINGS["save_credentials"] = True
        GoogleAuth.DEFAULT_SETTINGS["save_credentials_backend"] = "file"
        GoogleAuth.DEFAULT_SETTINGS[
            "save_credentials_file"
        ] = self.gdrive_user_credentials_path
        GoogleAuth.DEFAULT_SETTINGS["get_refresh_token"] = True
        GoogleAuth.DEFAULT_SETTINGS["oauth_scope"] = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.appdata",
        ]

        # Pass non existent settings path to force DEFAULT_SETTINGS loading
        gauth = GoogleAuth(settings_file="")
        gauth.CommandLineAuth()

        if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA):
            os.remove(self.gdrive_user_credentials_path)

        gdrive = GoogleDrive(gauth)
        return gdrive

    def create_drive_item(self, parent_id, title):
        upload_request = RequestCreateFolder(
            {"drive": self.drive, "title": title, "parent_id": parent_id}
        )
        result = self.execute_request(upload_request)
        return result

    def get_drive_item(self, name, parents_ids):
        if not parents_ids:
            return None
        query = " or ".join(
            "'{}' in parents".format(parent_id) for parent_id in parents_ids
        )

        query += " and trashed=false and title='{}'".format(name)

        list_request = RequestListFile(self.drive, query)
        item_list = self.execute_request(list_request)
        return next(iter(item_list), None)

    def resolve_remote_file(self, parents_ids, path_parts, create):
        for path_part in path_parts:
            item = self.get_drive_item(path_part, parents_ids)
            if not item and create:
                item = self.create_drive_item(parents_ids[0], path_part)
            elif not item:
                return None
            parents_ids = [item["id"]]
        return item

    def subtract_root_path(self, parts):
        if not hasattr(self, "root_id"):
            return parts, [self.path_info.bucket]

        for part in self.path_info.path.split("/"):
            if parts and parts[0] == part:
                parts.pop(0)
            else:
                break
        return parts, [self.root_id]

    def get_path_id_from_cache(self, path_info):
        files_ids = []
        parts, parents_ids = self.subtract_root_path(path_info.path.split("/"))
        if (
            hasattr(self, "cached_dirs")
            and path_info != self.path_info
            and parts
            and (parts[0] in self.cached_dirs)
        ):
            parents_ids = self.cached_dirs[parts[0]]
            files_ids = self.cached_dirs[parts[0]]
            parts.pop(0)

        return files_ids, parents_ids, parts

    def get_path_id(self, path_info, create=False):
        files_ids, parents_ids, parts = self.get_path_id_from_cache(path_info)

        if not parts and files_ids:
            return files_ids[0]

        file1 = self.resolve_remote_file(parents_ids, parts, create)
        return file1["id"] if file1 else ""

    def exists(self, path_info):
        return self.get_path_id(path_info) != ""

    def _upload(self, from_file, to_info, name, no_progress_bar):
        dirname = to_info.parent
        if dirname:
            parent_id = self.get_path_id(dirname, True)
        else:
            parent_id = to_info.bucket

        upload_request = RequestUploadFile(
            {
                "drive": self.drive,
                "title": to_info.name,
                "parent_id": parent_id,
            },
            no_progress_bar,
            from_file,
            name,
        )
        self.execute_request(upload_request)

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self.get_path_id(from_info)
        download_request = RequestDownloadFile(
            {
                "drive": self.drive,
                "file_id": file_id,
                "to_file": to_file,
                "progress_name": name,
                "no_progress_bar": no_progress_bar,
            }
        )
        self.execute_request(download_request)

    def list_cache_paths(self):
        file_id = self.get_path_id(self.path_info)
        prefix = self.path_info.path
        for path in self.list_path(file_id):
            yield posixpath.join(prefix, path)

    def list_file_path(self, drive_file):
        if drive_file["mimeType"] == FOLDER_MIME_TYPE:
            for i in self.list_path(drive_file["id"]):
                yield posixpath.join(drive_file["title"], i)
        else:
            yield drive_file["title"]

    def list_path(self, parent_id):
        for file1 in self.list_drive_item(
            "'{}' in parents and trashed=false".format(parent_id)
        ):
            for path in self.list_file_path(file1):
                yield path

    def all(self):
        if not hasattr(self, "cached_ids") or not self.cached_ids:
            return

        query = " or ".join(
            "'{}' in parents".format(dir_id) for dir_id in self.cached_ids
        )

        query += " and trashed=false"
        for file1 in self.list_drive_item(query):
            parent_id = file1["parents"][0]["id"]
            path = posixpath.join(self.cached_ids[parent_id], file1["title"])
            try:
                yield self.path_to_checksum(path)
            except ValueError:
                # We ignore all the non-cache looking files
                logger.debug('Ignoring path as "non-cache looking"')
