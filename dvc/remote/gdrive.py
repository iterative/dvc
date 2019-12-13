from __future__ import unicode_literals

import os
import posixpath
import logging
import threading

from funcy import retry, compose, decorator, wrap_with
from funcy.py3 import cat

from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.utils import tmp_fname

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveRetriableError(DvcException):
    def __init__(self, msg):
        super(GDriveRetriableError, self).__init__(msg)


@decorator
def _wrap_pydrive_retriable(call):
    from apiclient import errors
    from pydrive.files import ApiRequestError

    try:
        result = call()
    except (ApiRequestError, errors.HttpError) as exception:
        retry_codes = ["403", "500", "502", "503", "504"]
        if any(
            "HttpError {}".format(code) in str(exception)
            for code in retry_codes
        ):
            raise GDriveRetriableError(msg="Google API request failed")
        raise
    return result


gdrive_retry = compose(
    # 8 tries, start at 0.5s, multiply by golden ratio, cap at 10s
    retry(
        8, GDriveRetriableError, timeout=lambda a: min(0.5 * 1.618 ** a, 10)
    ),
    _wrap_pydrive_retriable,
)


class RemoteGDrive(RemoteBASE):
    scheme = Schemes.GDRIVE
    path_cls = CloudURLInfo
    REQUIRES = {"pydrive": "pydrive"}
    GDRIVE_USER_CREDENTIALS_DATA = "GDRIVE_USER_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = "gdrive-user-credentials.json"

    def __init__(self, repo, config):
        super(RemoteGDrive, self).__init__(repo, config)
        self.no_traverse = False
        self.path_info = self.path_cls(config[Config.SECTION_REMOTE_URL])
        self.config = config
        self.init_drive()

    def init_drive(self):
        self.client_id = self.config.get(Config.SECTION_GDRIVE_CLIENT_ID, None)
        self.client_secret = self.config.get(
            Config.SECTION_GDRIVE_CLIENT_SECRET, None
        )
        if not self.client_id or not self.client_secret:
            raise DvcException(
                "Please specify Google Drive's client id and "
                "secret in DVC's config. Learn more at "
                "https://man.dvc.org/remote/add."
            )
        self.gdrive_user_credentials_path = (
            tmp_fname(".dvc/tmp/")
            if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA)
            else self.config.get(
                Config.SECTION_GDRIVE_USER_CREDENTIALS_FILE,
                os.path.join(
                    self.repo.tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE
                ),
            )
        )

    @gdrive_retry
    def gdrive_upload_file(
        self, args, no_progress_bar=True, from_file="", progress_name=""
    ):
        item = self.drive.CreateFile(
            {"title": args["title"], "parents": [{"id": args["parent_id"]}]}
        )

        with open(from_file, "rb") as fobj:
            total = os.path.getsize(from_file)
            with Tqdm.wrapattr(
                fobj,
                "read",
                desc=progress_name,
                total=total,
                disable=no_progress_bar,
            ) as wrapped:
                # PyDrive doesn't like content property setting for empty files
                # https://github.com/gsuitedevs/PyDrive/issues/121
                if total:
                    item.content = wrapped
                item.Upload()
        return item

    @gdrive_retry
    def gdrive_download_file(
        self, file_id, to_file, progress_name, no_progress_bar
    ):
        gdrive_file = self.drive.CreateFile({"id": file_id})
        bar_format = (
            "Donwloading {desc:{ncols_desc}.{ncols_desc}}... "
            + Tqdm.format_sizeof(int(gdrive_file["fileSize"]), "B", 1024)
        )
        with Tqdm(
            bar_format=bar_format, desc=progress_name, disable=no_progress_bar
        ):
            gdrive_file.GetContentFile(to_file)

    def gdrive_list_item(self, query):
        file_list = self.drive.ListFile({"q": query, "maxResults": 1000})

        # Isolate and decorate fetching of remote drive items in pages
        get_list = gdrive_retry(lambda: next(file_list, None))

        # Fetch pages until None is received, lazily flatten the thing
        return cat(iter(get_list, None))

    def cache_root_dirs(self):
        cached_dirs = {}
        cached_ids = {}
        for dir1 in self.gdrive_list_item(
            "'{}' in parents and trashed=false".format(self.root_id)
        ):
            remote_path = posixpath.join(self.path_info.path, dir1["title"])
            cached_dirs.setdefault(remote_path, []).append(dir1["id"])
            cached_ids[dir1["id"]] = dir1["title"]
        return cached_dirs, cached_ids

    @property
    def cached_dirs(self):
        if not hasattr(self, "_cached_dirs"):
            self.drive
        return self._cached_dirs

    @property
    def cached_ids(self):
        if not hasattr(self, "_cached_ids"):
            self.drive
        return self._cached_ids

    @property
    @wrap_with(threading.RLock())
    def drive(self):
        if not hasattr(self, "_gdrive"):
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
                "client_id": self.client_id,
                "client_secret": self.client_secret,
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

            self._gdrive = GoogleDrive(gauth)

            self.root_id = self.get_remote_id(self.path_info, create=True)
            self._cached_dirs, self._cached_ids = self.cache_root_dirs()

        return self._gdrive

    @gdrive_retry
    def create_remote_dir(self, parent_id, title):
        item = self.drive.CreateFile(
            {
                "title": title,
                "parents": [{"id": parent_id}],
                "mimeType": FOLDER_MIME_TYPE,
            }
        )
        item.Upload()
        return item

    @gdrive_retry
    def get_remote_item(self, name, parents_ids):
        if not parents_ids:
            return None
        query = " or ".join(
            "'{}' in parents".format(parent_id) for parent_id in parents_ids
        )

        query += " and trashed=false and title='{}'".format(name)

        # Limit found remote items count to 1 in response
        item_list = self.drive.ListFile(
            {"q": query, "maxResults": 1}
        ).GetList()
        return next(iter(item_list), None)

    def resolve_remote_item_from_path(self, path_parts, create):
        parents_ids = ["root"]
        current_path = ""
        for path_part in path_parts:
            current_path = posixpath.join(current_path, path_part)
            remote_ids = self.get_remote_id_from_cache(current_path)
            if remote_ids:
                parents_ids = remote_ids
                continue
            item = self.get_remote_item(path_part, parents_ids)
            if not item and create:
                item = self.create_remote_dir(parents_ids[0], path_part)
            elif not item:
                return None
            parents_ids = [item["id"]]
        return item

    def get_remote_id_from_cache(self, remote_path):
        if hasattr(self, "_cached_dirs"):
            return self.cached_dirs.get(remote_path, [])
        return []

    def get_remote_id(self, path_info, create=False):
        remote_ids = self.get_remote_id_from_cache(path_info.path)

        if remote_ids:
            return remote_ids[0]

        file1 = self.resolve_remote_item_from_path(
            path_info.path.split("/"), create
        )
        return file1["id"] if file1 else ""

    def exists(self, path_info):
        return self.get_remote_id(path_info) != ""

    def _upload(self, from_file, to_info, name, no_progress_bar):
        dirname = to_info.parent
        if dirname:
            parent_id = self.get_remote_id(dirname, True)
        else:
            parent_id = to_info.bucket

        self.gdrive_upload_file(
            {"title": to_info.name, "parent_id": parent_id},
            no_progress_bar,
            from_file,
            name,
        )

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self.get_remote_id(from_info)
        self.gdrive_download_file(file_id, to_file, name, no_progress_bar)

    def all(self):
        if not self.cached_ids:
            return

        query = " or ".join(
            "'{}' in parents".format(dir_id) for dir_id in self.cached_ids
        )

        query += " and trashed=false"
        for file1 in self.gdrive_list_item(query):
            parent_id = file1["parents"][0]["id"]
            path = posixpath.join(self.cached_ids[parent_id], file1["title"])
            try:
                yield self.path_to_checksum(path)
            except ValueError:
                # We ignore all the non-cache looking files
                logger.debug('Ignoring path as "non-cache looking"')
