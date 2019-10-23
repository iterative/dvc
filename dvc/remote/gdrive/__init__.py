from __future__ import unicode_literals

import os
import posixpath

from funcy import cached_property
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.remote.gdrive.utils import shared_token_warning
from dvc.exceptions import DvcException
from dvc.remote.gdrive.pydrive import (
    RequestListFile,
    RequestListFilePaginated,
    RequestUploadFile,
    RequestDownloadFile,
)


class GDriveURLInfo(CloudURLInfo):
    @property
    def netloc(self):
        return self.parsed.netloc


class RemoteGDrive(RemoteBASE):
    scheme = Schemes.GDRIVE
    path_cls = GDriveURLInfo
    REGEX = r"^gdrive://.*$"
    REQUIRES = {"pydrive": "pydrive"}
    PARAM_CHECKSUM = "md5Checksum"
    DEFAULT_GOOGLE_AUTH_SETTINGS_PATH = os.path.join(
        os.path.dirname(__file__), "settings.yaml"
    )
    FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.no_traverse = False
        if Config.SECTION_GDRIVE_CREDENTIALPATH not in config:
            shared_token_warning()
        self.gdrive_credentials_path = config.get(
            Config.SECTION_GDRIVE_CREDENTIALPATH,
            self.DEFAULT_GOOGLE_AUTH_SETTINGS_PATH,
        )
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.init_drive()

    def init_drive(self):
        self.root_id = self.get_path_id(self.path_info, create=True)

    @on_exception(expo, DvcException, max_tries=8)
    @sleep_and_retry
    @limits(calls=10, period=1)
    def execute_request(self, request):
        try:
            result = request.execute()
        except Exception as exception:
            if "Rate Limit Exceeded" in str(exception):
                raise DvcException("API usage rate limit exceeded")
            raise
        return result

    def list_drive_item(self, query):
        list_request = RequestListFilePaginated(self.drive, query)
        page_list = self.execute_request(list_request)
        while page_list:
            for item in page_list:
                yield item
            page_list = self.execute_request(list_request)

    @cached_property
    def cached_root_dirs(self):
        cached_dirs = {}
        self.cached_ids = {}
        for dir1 in self.list_drive_item(
            "'{}' in parents and trashed=false".format(self.root_id)
        ):
            cached_dirs[dir1["title"]] = dir1["id"]
            self.cached_ids[dir1["id"]] = dir1["title"]
        return cached_dirs

    @cached_property
    def drive(self):
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        import logging

        if os.getenv("PYDRIVE_USER_CREDENTIALS_DATA"):
            with open("credentials.json", "w") as credentials_file:
                credentials_file.write(
                    os.getenv("PYDRIVE_USER_CREDENTIALS_DATA")
                )

        # Supress import error on GoogleAuth warning
        logging.getLogger("googleapiclient.discovery_cache").setLevel(
            logging.ERROR
        )

        GoogleAuth.DEFAULT_SETTINGS["client_config_backend"] = "settings"
        gauth = GoogleAuth(settings_file=self.gdrive_credentials_path)
        gauth.CommandLineAuth()
        gdrive = GoogleDrive(gauth)
        return gdrive

    def create_drive_item(self, parent_id, title):
        upload_request = RequestUploadFile(
            {
                "drive": self.drive,
                "title": title,
                "parent_id": parent_id,
                "mime_type": self.FOLDER_MIME_TYPE,
            }
        )
        result = self.execute_request(upload_request)
        return result

    def get_drive_item(self, name, parent_id):
        list_request = RequestListFile(
            self.drive,
            "'{}' in parents and trashed=false and title='{}'".format(
                parent_id, name
            ),
        )
        item_list = self.execute_request(list_request)
        return next(iter(item_list), None)

    def resolve_remote_file(self, parent_id, path_parts, create):
        for path_part in path_parts:
            item = self.get_drive_item(path_part, parent_id)
            if not item and create:
                item = self.create_drive_item(parent_id, path_part)
            elif not item:
                return None
            parent_id = item["id"]
        return item

    def get_path_id_from_cache(self, path_info):
        file_id = ""
        parts = path_info.path.split("/")
        if (
            path_info != self.path_info
            and parts
            and (parts[0] in self.cached_root_dirs)
        ):
            parent_id = self.cached_root_dirs[parts[0]]
            file_id = self.cached_root_dirs[parts[0]]
            parts.pop(0)
        else:
            parent_id = path_info.netloc
        return file_id, parent_id, parts

    def get_path_id(self, path_info, create=False):
        file_id, parent_id, parts = self.get_path_id_from_cache(path_info)

        if not parts and file_id:
            return file_id

        file1 = self.resolve_remote_file(parent_id, parts, create)
        return file1["id"] if file1 else ""

    def exists(self, path_info):
        return self.get_path_id(path_info) != ""

    def _upload(self, from_file, to_info, name, no_progress_bar):
        dirname = to_info.parent
        if dirname:
            parent_id = self.get_path_id(dirname, True)
        else:
            parent_id = to_info.netloc

        upload_request = RequestUploadFile(
            {
                "drive": self.drive,
                "title": to_info.name,
                "parent_id": parent_id,
                "mime_type": "",
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
        if drive_file["mimeType"] == self.FOLDER_MIME_TYPE:
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
        query = " or ".join(
            "'{}' in parents".format(dir_id)
            for dir_title, dir_id in self.cached_root_dirs.items()
        )
        if not query:
            return
        query += " and trashed=false"
        print("All query: {}".format(query))
        counter = 0
        for file1 in self.list_drive_item(query):
            parent_id = file1["parents"][0]["id"]
            print(self.cached_ids[parent_id])
            print(file1["title"])
            counter += 1
            print("{}".format(counter))
            path = posixpath.join(self.cached_ids[parent_id], file1["title"])
            try:
                yield self.path_to_checksum(path)
            except ValueError:
                # We ignore all the non-cache looking files
                pass
