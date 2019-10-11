from __future__ import unicode_literals

import os

from funcy import cached_property
import ratelimit

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.remote.gdrive.utils import TrackFileReadProgress, shared_token_warning


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
        if Config.SECTION_GDRIVE_CREDENTIALPATH not in config:
            shared_token_warning()
        self.gdrive_credentials_path = config.get(
            Config.SECTION_GDRIVE_CREDENTIALPATH,
            self.DEFAULT_GOOGLE_AUTH_SETTINGS_PATH,
        )
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self._drive = None

    @cached_property
    def cached_root_dirs(self):
        cached_dirs = {}
        for dirs_list in self.drive.ListFile(
            {
                "q": "'%s' in parents and trashed=false"
                % self.path_info.netloc,
                "maxResults": 256,
            }
        ):
            for dir1 in dirs_list:
                cached_dirs[dir1["title"]] = dir1["id"]
        return cached_dirs

    @property
    def raw_drive(self):
        from pydrive.auth import GoogleAuth
        from pydrive.drive import GoogleDrive
        import logging

        if self._drive is None:
            if os.getenv("PYDRIVE_USER_CREDENTIALS_FILE_CONTENT"):
                with open("credentials.json", "w") as credentials_file:
                    credentials_file.write(
                        os.getenv("PYDRIVE_USER_CREDENTIALS_FILE_CONTENT")
                    )

            logging.getLogger("googleapiclient.discovery_cache").setLevel(
                logging.ERROR
            )

            GoogleAuth.DEFAULT_SETTINGS["client_config_backend"] = "settings"
            gauth = GoogleAuth(settings_file=self.gdrive_credentials_path)
            gauth.CommandLineAuth()
            self._drive = GoogleDrive(gauth)

            self.get_path_id(self.path_info, create=True)
        return self._drive

    @property
    @ratelimit.sleep_and_retry
    @ratelimit.limits(calls=8, period=10)
    def drive(self):
        return self.raw_drive

    def resolve_file_id_from_part(self, part, parent_id, file_list):
        file_id = ""
        for file1 in file_list:
            if file1["title"] == part:
                file_id = file1["id"]
                file_list = self.drive.ListFile(
                    {"q": "'%s' in parents and trashed=false" % file_id}
                ).GetList()
                parent_id = file1["id"]
                break
        return file_id, parent_id, file_list

    def create_file_id(self, file_id, parent_id, part, create):
        if file_id == "":
            if create:
                gdrive_file = self.drive.CreateFile(
                    {
                        "title": part,
                        "parents": [{"id": parent_id}],
                        "mimeType": self.FOLDER_MIME_TYPE,
                    }
                )
                gdrive_file.Upload()
                file_id = gdrive_file["id"]
        return file_id

    def resolve_file_id(self, file_id, parent_id, path_parts, create):
        file_list = self.drive.ListFile(
            {"q": "'%s' in parents and trashed=false" % parent_id}
        ).GetList()

        for part in path_parts:
            file_id, parent_id, file_list = self.resolve_file_id_from_part(
                part, parent_id, file_list
            )
            file_id = self.create_file_id(file_id, parent_id, part, create)
            if file_id == "":
                break
        return file_id

    def get_path_id(self, path_info, create=False):
        file_id = ""
        parts = path_info.path.split("/")

        if parts and (parts[0] in self.cached_root_dirs):
            parent_id = self.cached_root_dirs[parts[0]]
            file_id = self.cached_root_dirs[parts[0]]
            parts.pop(0)
        else:
            parent_id = path_info.netloc

        if not parts and file_id:
            return file_id

        return self.resolve_file_id(file_id, parent_id, parts, create)

    def exists(self, path_info):
        return self.get_path_id(path_info) != ""

    def batch_exists(self, path_infos, callback):
        results = []
        for path_info in path_infos:
            results.append(self.exists(path_info))
            callback.update(str(path_info))
        return results

    def _upload(self, from_file, to_info, name, no_progress_bar):

        dirname = to_info.parent
        if dirname:
            parent_id = self.get_path_id(dirname, True)
        else:
            parent_id = to_info.netloc

        file1 = self.drive.CreateFile(
            {"title": to_info.name, "parents": [{"id": parent_id}]}
        )

        from_file = open(from_file, "rb")
        if not no_progress_bar:
            from_file = TrackFileReadProgress(name, from_file)

        file1.content = from_file

        file1.Upload()
        from_file.close()

    def _download(self, from_info, to_file, name, no_progress_bar):
        from dvc.progress import Tqdm

        file_id = self.get_path_id(from_info)
        gdrive_file = self.drive.CreateFile({"id": file_id})
        if not no_progress_bar:
            tqdm = Tqdm(desc=name, total=int(gdrive_file["fileSize"]))
        gdrive_file.GetContentFile(to_file)
        if not no_progress_bar:
            tqdm.close()

    def get_file_checksum(self, path_info):
        raise NotImplementedError

    def list_cache_paths(self):
        file_id = self.get_path_id(self.path_info)
        prefix = self.path_info.path
        for path in self.list_path(file_id):
            yield prefix + "/" + path

    def walk(self, path_info):
        raise NotImplementedError

    def list_path(self, parent_id):
        file_list = self.drive.ListFile(
            {"q": "'%s' in parents and trashed=false" % parent_id}
        ).GetList()
        for file1 in file_list:
            if file1["mimeType"] == self.FOLDER_MIME_TYPE:
                for i in self.list_path(file1["id"]):
                    yield file1["title"] + "/" + i
            else:
                yield file1["title"]
