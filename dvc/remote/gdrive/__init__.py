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
from dvc.remote.gdrive.utils import TrackFileReadProgress, shared_token_warning
from dvc.exceptions import DvcException

class GDriveURLInfo(CloudURLInfo):
    @property
    def netloc(self):
        return self.parsed.netloc


class RequestBASE:
    def __init__(self, drive):
        self.drive = drive

    def execute(self):
        raise NotImplementedError


class RequestListFile(RequestBASE):
    def __init__(self, drive, query):
        super(RequestListFile, self).__init__(drive)
        self.query = query

    def execute(self):
        return self.drive.ListFile({"q": self.query, "maxResults": 1000}).GetList()


class RequestUploadFile(RequestBASE):
    def __init__(
        self,
        drive,
        title,
        parent_id,
        mime_type,
        no_progress_bar=True,
        from_file="",
        progress_name="",
    ):
        super(RequestUploadFile, self).__init__(drive)
        self.title = title
        self.parent_id = parent_id
        self.mime_type = mime_type
        self.no_progress_bar = no_progress_bar
        self.from_file = from_file
        self.proress_name = progress_name

    def execute(self):
        item = self.drive.CreateFile(
            {
                "title": self.title,
                "parents": [{"id": self.parent_id}],
                "mimeType": self.mime_type,
            }
        )
        if self.mime_type == RemoteGDrive.FOLDER_MIME_TYPE:
            item.Upload()
        else:
            with open(self.from_file, "rb") as from_file:
                if not self.no_progress_bar:
                    from_file = TrackFileReadProgress(
                        self.proress_name, from_file
                    )
                if os.stat(self.from_file).st_size:
                    item.content = from_file
                item.Upload()
        return item


class RequestDownloadFile(RequestBASE):
    def __init__(
        self, drive, file_id, to_file, progress_name, no_progress_bar=True
    ):
        super(RequestDownloadFile, self).__init__(drive)
        self.file_id = file_id
        self.to_file = to_file
        self.progress_name = progress_name
        self.no_progress_bar = no_progress_bar

    def execute(self):
        from dvc.progress import Tqdm

        gdrive_file = self.drive.CreateFile({"id": self.file_id})
        if not self.no_progress_bar:
            tqdm = Tqdm(
                desc=self.progress_name, total=int(gdrive_file["fileSize"])
            )
        gdrive_file.GetContentFile(self.to_file)
        if not self.no_progress_bar:
            tqdm.close()


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
            if ('Rate Limit Exceeded' in str(exception)):
                raise DvcException("API usage rate limit exceeded")
            raise
        return result

    def list_drive_item(self, query):
        list_request = RequestListFile(self.drive, query)
        for item in self.execute_request(list_request):
            yield item
        #for page in self.execute_request(list_request):
        #    for item in page:
        #        yield item

    @cached_property
    def cached_root_dirs(self):
        self.cached_dirs = {}
        self.cached_dir_id = {}
        for dir1 in self.list_drive_item(
            "'{}' in parents and trashed=false".format(self.root_id)
        ):
            self.cached_dirs[dir1["title"]] = dir1["id"]
            self.cached_dir_id[dir1["id"]] = dir1["title"]
        return self.cached_dirs

    @cached_property
    def raw_drive(self):
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

    @property
    def drive(self):
        return self.raw_drive

    def create_drive_item(self, parent_id, title):
        upload_request = RequestUploadFile(
            self.drive, title, parent_id, self.FOLDER_MIME_TYPE
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
            self.drive,
            to_info.name,
            parent_id,
            "",
            no_progress_bar,
            from_file,
            name,
        )
        self.execute_request(upload_request)

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self.get_path_id(from_info)
        download_request = RequestDownloadFile(
            self.drive, file_id, to_file, name, no_progress_bar
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
        for file1 in self.list_drive_item(query):
            parent_id = file1["parents"][0]["id"]
            print(self.cached_dir_id[parent_id])
            print(file1["title"])
            path = posixpath.join(
                self.cached_dir_id[parent_id], file1["title"]
            )
            try:
                yield self.path_to_checksum(path)
            except ValueError:
                # We ignore all the non-cache looking files
                pass
