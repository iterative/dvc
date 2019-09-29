from __future__ import unicode_literals

import os
import logging

try:
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
except ImportError:
    GoogleAuth = None
    GoogleDrive = None

from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.remote.gdrive.utils import TrackFileReadProgress
from dvc.progress import Tqdm


logger = logging.getLogger(__name__)


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
    GOOGLE_AUTH_SETTINGS_PATH = os.path.join(
        os.path.dirname(__file__), "settings.yaml"
    )

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.root_content_cached = False
        self.root_dirs_list = {}
        self.init_gdrive()

    def init_gdrive(self):
        self.gdrive = self.drive()
        self.cache_root_content()

    def drive(self):
        GoogleAuth.DEFAULT_SETTINGS["client_config_backend"] = "settings"
        gauth = GoogleAuth(settings_file=self.GOOGLE_AUTH_SETTINGS_PATH)
        gauth.CommandLineAuth()
        return GoogleDrive(gauth)

    def cache_root_content(self):
        if not self.root_content_cached:
            for dirs_list in self.gdrive.ListFile(
                {
                    "q": "'%s' in parents and trashed=false"
                    % self.path_info.netloc,
                    "maxResults": 256,
                }
            ):
                for dir1 in dirs_list:
                    self.root_dirs_list[dir1["title"]] = dir1["id"]
            self.root_content_cached = True

    def get_path_id(self, path_info, create=False):
        file_id = ""
        parts = path_info.path.split("/")

        if parts and (parts[0] in self.root_dirs_list):
            parent_id = self.root_dirs_list[parts[0]]
            file_id = self.root_dirs_list[parts[0]]
            parts.pop(0)
        else:
            parent_id = path_info.netloc
        file_list = self.gdrive.ListFile(
            {"q": "'%s' in parents and trashed=false" % parent_id}
        ).GetList()

        for part in parts:
            file_id = ""
            for f in file_list:
                if f["title"] == part:
                    file_id = f["id"]
                    file_list = self.gdrive.ListFile(
                        {"q": "'%s' in parents and trashed=false" % file_id}
                    ).GetList()
                    parent_id = f["id"]
                    break
            if file_id == "":
                if create:
                    gdrive_file = self.gdrive.CreateFile(
                        {
                            "title": part,
                            "parents": [{"id": parent_id}],
                            "mimeType": "application/vnd.google-apps.folder",
                        }
                    )
                    gdrive_file.Upload()
                    file_id = gdrive_file["id"]
                else:
                    break
        return file_id

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

        file1 = self.gdrive.CreateFile(
            {"title": to_info.name, "parents": [{"id": parent_id}]}
        )

        from_file = open(from_file, "rb")
        if not no_progress_bar:
            from_file = TrackFileReadProgress(name, from_file)

        file1.content = from_file
        file1.Upload()
        from_file.close()

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self.get_path_id(from_info)
        gdrive_file = self.gdrive.CreateFile({"id": file_id})
        gdrive_file.GetContentFile(to_file)
        #if not no_progress_bar:
        #    progress.update_target(name, 1, 1)
