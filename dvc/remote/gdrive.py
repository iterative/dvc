from collections import defaultdict
import os
import posixpath
import logging
import re
import threading
from urllib.parse import urlparse

from funcy import retry, wrap_with, wrap_prop, cached_property
from funcy.py3 import cat

from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.exceptions import DvcException
from dvc.utils import tmp_fname, format_link

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDrivePathNotFound(DvcException):
    def __init__(self, path_info):
        super().__init__("Google Drive path '{}' not found.".format(path_info))


class GDriveAccessTokenRefreshError(DvcException):
    def __init__(self):
        super().__init__("Google Drive access token refreshment is failed.")


class GDriveMissedCredentialKeyError(DvcException):
    def __init__(self, path):
        super().__init__(
            "Google Drive user credentials file '{}' "
            "misses value for key.".format(path)
        )


def gdrive_retry(func):
    retry_re = re.compile(r"HttpError (403|500|502|503|504)")

    def should_retry(exc):
        from pydrive2.files import ApiRequestError

        return isinstance(exc, ApiRequestError) and retry_re.search(str(exc))

    # 15 tries, start at 0.5s, multiply by golden ratio, cap at 20s
    return retry(
        15,
        timeout=lambda a: min(0.5 * 1.618 ** a, 20),
        filter_errors=should_retry,
    )(func)


class GDriveURLInfo(CloudURLInfo):
    def __init__(self, url):
        super().__init__(url)

        # GDrive URL host part is case sensitive,
        # we are restoring it here.
        p = urlparse(url)
        self.host = p.netloc
        assert self.netloc == self.host

        # Normalize path. Important since we have a cache (path to ID)
        # and don't want to deal with different variations of path in it.
        self._spath = re.sub("/{2,}", "/", self._spath.rstrip("/"))


class RemoteGDrive(RemoteBASE):
    scheme = Schemes.GDRIVE
    path_cls = GDriveURLInfo
    REQUIRES = {"pydrive2": "pydrive2"}
    DEFAULT_NO_TRAVERSE = False
    DEFAULT_VERIFY = True

    GDRIVE_USER_CREDENTIALS_DATA = "GDRIVE_USER_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = "gdrive-user-credentials.json"

    def __init__(self, repo, config):
        super().__init__(repo, config)
        self.path_info = self.path_cls(config["url"])

        if not self.path_info.bucket:
            raise DvcException(
                "Empty Google Drive URL '{}'. Learn more at "
                "{}.".format(
                    config["url"],
                    format_link("https://man.dvc.org/remote/add"),
                )
            )

        self._bucket = self.path_info.bucket
        self._client_id = config.get("gdrive_client_id")
        self._client_secret = config.get("gdrive_client_secret")
        if not self._client_id or not self._client_secret:
            raise DvcException(
                "Please specify Google Drive's client id and "
                "secret in DVC config. Learn more at "
                "{}.".format(format_link("https://man.dvc.org/remote/add"))
            )
        self._gdrive_user_credentials_path = (
            tmp_fname(os.path.join(self.repo.tmp_dir, ""))
            if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA)
            else config.get(
                "gdrive_user_credentials_file",
                os.path.join(
                    self.repo.tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE
                ),
            )
        )

        self._list_params = None

    @wrap_prop(threading.RLock())
    @cached_property
    def drive(self):
        from pydrive2.auth import RefreshError
        from pydrive2.auth import GoogleAuth
        from pydrive2.drive import GoogleDrive

        if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA):
            with open(
                self._gdrive_user_credentials_path, "w"
            ) as credentials_file:
                credentials_file.write(
                    os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA)
                )

        GoogleAuth.DEFAULT_SETTINGS["client_config_backend"] = "settings"
        GoogleAuth.DEFAULT_SETTINGS["client_config"] = {
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "revoke_uri": "https://oauth2.googleapis.com/revoke",
            "redirect_uri": "",
        }
        GoogleAuth.DEFAULT_SETTINGS["save_credentials"] = True
        GoogleAuth.DEFAULT_SETTINGS["save_credentials_backend"] = "file"
        GoogleAuth.DEFAULT_SETTINGS[
            "save_credentials_file"
        ] = self._gdrive_user_credentials_path
        GoogleAuth.DEFAULT_SETTINGS["get_refresh_token"] = True
        GoogleAuth.DEFAULT_SETTINGS["oauth_scope"] = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.appdata",
        ]

        # Pass non existent settings path to force DEFAULT_SETTINGS loading
        gauth = GoogleAuth(settings_file="")

        try:
            gauth.CommandLineAuth()
        except RefreshError as exc:
            raise GDriveAccessTokenRefreshError from exc
        except KeyError as exc:
            raise GDriveMissedCredentialKeyError(
                self._gdrive_user_credentials_path
            ) from exc
        # Handle pydrive2.auth.AuthenticationError and other auth failures
        except Exception as exc:
            raise DvcException("Google Drive authentication failed") from exc
        finally:
            if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA):
                os.remove(self._gdrive_user_credentials_path)

        return GoogleDrive(gauth)

    @wrap_prop(threading.RLock())
    @cached_property
    def cache(self):
        cache = {"dirs": defaultdict(list), "ids": {}}

        cache["root_id"] = self._get_remote_id(self.path_info)
        cache["dirs"][self.path_info.path] = [cache["root_id"]]
        self._cache_path(self.path_info.path, cache["root_id"], cache)

        for item in self.gdrive_list_item(
            "'{}' in parents and trashed=false".format(cache["root_id"])
        ):
            remote_path = (self.path_info / item["title"]).path
            self._cache_path(remote_path, item["id"], cache)

        return cache

    def _cache_path(self, remote_path, remote_id, cache=None):
        cache = cache or self.cache
        cache["dirs"][remote_path].append(remote_id)
        cache["ids"][remote_id] = remote_path

    @cached_property
    def list_params(self):
        params = {"corpora": "default"}
        if self._bucket != "root" and self._bucket != "appDataFolder":
            params["driveId"] = self._get_remote_drive_id(self._bucket)
            params["corpora"] = "drive"
        return params

    @gdrive_retry
    def gdrive_upload_file(
        self,
        parent_id,
        title,
        no_progress_bar=False,
        from_file="",
        progress_name="",
    ):
        item = self.drive.CreateFile(
            {"title": title, "parents": [{"id": parent_id}]}
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
        param = {"id": file_id}
        # it does not create a file on the remote
        gdrive_file = self.drive.CreateFile(param)
        bar_format = (
            "Downloading {desc:{ncols_desc}.{ncols_desc}}... "
            + Tqdm.format_sizeof(int(gdrive_file["fileSize"]), "B", 1024)
        )
        with Tqdm(
            bar_format=bar_format, desc=progress_name, disable=no_progress_bar
        ):
            gdrive_file.GetContentFile(to_file)

    def gdrive_list_item(self, query):
        param = {"q": query, "maxResults": 1000}
        param.update(self.list_params)

        file_list = self.drive.ListFile(param)

        # Isolate and decorate fetching of remote drive items in pages
        get_list = gdrive_retry(lambda: next(file_list, None))

        # Fetch pages until None is received, lazily flatten the thing
        return cat(iter(get_list, None))

    @wrap_with(threading.RLock())
    def gdrive_create_dir(self, parent_id, title, remote_path):
        cached = self.cache["dirs"].get(remote_path)
        if cached:
            return cached[0]

        item = self._create_remote_dir(parent_id, title)

        if parent_id == self.cache["root_id"]:
            self._cache_path(remote_path, item["id"])

        return item["id"]

    @gdrive_retry
    def _create_remote_dir(self, parent_id, title):
        parent = {"id": parent_id}
        item = self.drive.CreateFile(
            {"title": title, "parents": [parent], "mimeType": FOLDER_MIME_TYPE}
        )
        item.Upload()
        return item

    @gdrive_retry
    def _delete_remote_file(self, remote_id):
        param = {"id": remote_id}
        # it does not create a file on the remote
        item = self.drive.CreateFile(param)
        item.Delete()

    @gdrive_retry
    def _get_remote_item(self, name, parents_ids):
        if not parents_ids:
            return None
        query = "({})".format(
            " or ".join(
                "'{}' in parents".format(parent_id)
                for parent_id in parents_ids
            )
        )

        query += " and trashed=false and title='{}'".format(name)

        # Remote might contain items with duplicated path (titles).
        # We thus limit number of items.
        param = {"q": query, "maxResults": 1}
        param.update(self.list_params)

        # Limit found remote items count to 1 in response
        item_list = self.drive.ListFile(param).GetList()
        return next(iter(item_list), None)

    @gdrive_retry
    def _get_remote_drive_id(self, remote_id):
        param = {"id": remote_id}
        # it does not create a file on the remote
        item = self.drive.CreateFile(param)
        item.FetchMetadata("driveId")
        return item.get("driveId", None)

    def _get_cached_remote_ids(self, path):
        if not path:
            return [self._bucket]
        if "cache" in self.__dict__:
            return self.cache["dirs"].get(path, [])
        return []

    def _path_to_remote_ids(self, path, create):
        remote_ids = self._get_cached_remote_ids(path)
        if remote_ids:
            return remote_ids

        parent_path, part = posixpath.split(path)
        parent_ids = self._path_to_remote_ids(parent_path, create)
        item = self._get_remote_item(part, parent_ids)

        if not item:
            return (
                [self.gdrive_create_dir(parent_ids[0], part, path)]
                if create
                else []
            )

        return [item["id"]]

    def _get_remote_id(self, path_info, create=False):
        assert path_info.bucket == self._bucket

        remote_ids = self._path_to_remote_ids(path_info.path, create)
        if not remote_ids:
            raise GDrivePathNotFound(path_info)

        return remote_ids[0]

    def exists(self, path_info):
        try:
            self._get_remote_id(path_info)
        except GDrivePathNotFound:
            return False
        else:
            return True

    def _upload(self, from_file, to_info, name, no_progress_bar):
        dirname = to_info.parent
        assert dirname
        parent_id = self._get_remote_id(dirname, True)

        self.gdrive_upload_file(
            parent_id, to_info.name, no_progress_bar, from_file, name
        )

    def _download(self, from_info, to_file, name, no_progress_bar):
        file_id = self._get_remote_id(from_info)
        self.gdrive_download_file(file_id, to_file, name, no_progress_bar)

    def list_cache_paths(self):
        if not self.cache["ids"]:
            return

        parents_query = " or ".join(
            "'{}' in parents".format(dir_id) for dir_id in self.cache["ids"]
        )
        query = "({}) and trashed=false".format(parents_query)

        for item in self.gdrive_list_item(query):
            parent_id = item["parents"][0]["id"]
            yield posixpath.join(self.cache["ids"][parent_id], item["title"])

    def remove(self, path_info):
        remote_id = self._get_remote_id(path_info)
        self._delete_remote_file(remote_id)
