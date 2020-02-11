import os
import posixpath
import logging
import re
import threading
from urllib.parse import urlparse

from funcy import retry, compose, decorator, wrap_with
from funcy.py3 import cat

from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.path_info import CloudURLInfo
from dvc.remote.base import RemoteBASE
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.utils import tmp_fname, format_link

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveRetriableError(DvcException):
    pass


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


@decorator
def _wrap_pydrive_retriable(call):
    from pydrive2.files import ApiRequestError

    try:
        result = call()
    except ApiRequestError as exception:
        retry_codes = ["403", "500", "502", "503", "504"]
        if any(
            "HttpError {}".format(code) in str(exception)
            for code in retry_codes
        ):
            raise GDriveRetriableError("Google API request failed")
        raise
    return result


gdrive_retry = compose(
    # 15 tries, start at 0.5s, multiply by golden ratio, cap at 20s
    retry(
        15, GDriveRetriableError, timeout=lambda a: min(0.5 * 1.618 ** a, 20)
    ),
    _wrap_pydrive_retriable,
)


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
        url = config[Config.SECTION_REMOTE_URL]
        self.path_info = self.path_cls(url)
        self.config = config

        if not self.path_info.bucket:
            raise DvcException(
                "Empty Google Drive URL '{}'. Learn more at "
                "{}.".format(
                    url, format_link("https://man.dvc.org/remote/add")
                )
            )

        self._bucket = self.path_info.bucket
        self._client_id = self.config.get(
            Config.SECTION_GDRIVE_CLIENT_ID, None
        )
        self._client_secret = self.config.get(
            Config.SECTION_GDRIVE_CLIENT_SECRET, None
        )
        if not self._client_id or not self._client_secret:
            raise DvcException(
                "Please specify Google Drive's client id and "
                "secret in DVC config. Learn more at "
                "{}.".format(format_link("https://man.dvc.org/remote/add"))
            )
        self._gdrive_user_credentials_path = (
            tmp_fname(os.path.join(self.repo.tmp_dir, ""))
            if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA)
            else self.config.get(
                Config.SECTION_GDRIVE_USER_CREDENTIALS_FILE,
                os.path.join(
                    self.repo.tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE
                ),
            )
        )

        self._list_params = None
        self._gdrive = None

        self._cache_initialized = False
        self._remote_root_id = None
        self._cached_dirs = None
        self._cached_ids = None

    @property
    @wrap_with(threading.RLock())
    def drive(self):
        from pydrive2.auth import RefreshError

        if not self._gdrive:
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
                raise DvcException(
                    "Google Drive authentication failed"
                ) from exc
            finally:
                if os.getenv(RemoteGDrive.GDRIVE_USER_CREDENTIALS_DATA):
                    os.remove(self._gdrive_user_credentials_path)

            self._gdrive = GoogleDrive(gauth)

        return self._gdrive

    @wrap_with(threading.RLock())
    def _initialize_cache(self):
        if self._cache_initialized:
            return

        cached_dirs = {}
        cached_ids = {}
        self._remote_root_id = self._get_remote_id(self.path_info)
        for dir1 in self.gdrive_list_item(
            "'{}' in parents and trashed=false".format(self._remote_root_id)
        ):
            remote_path = posixpath.join(self.path_info.path, dir1["title"])
            cached_dirs.setdefault(remote_path, []).append(dir1["id"])
            cached_ids[dir1["id"]] = dir1["title"]

        self._cached_dirs = cached_dirs
        self._cached_ids = cached_ids
        self._cache_initialized = True

    @property
    def cached_dirs(self):
        if not self._cache_initialized:
            self._initialize_cache()
        return self._cached_dirs

    @property
    def cached_ids(self):
        if not self._cache_initialized:
            self._initialize_cache()
        return self._cached_ids

    @property
    def remote_root_id(self):
        if not self._cache_initialized:
            self._initialize_cache()
        return self._remote_root_id

    @property
    def list_params(self):
        if not self._list_params:
            params = {"corpora": "default"}
            if self._bucket != "root" and self._bucket != "appDataFolder":
                params["driveId"] = self._get_remote_drive_id(self._bucket)
                params["corpora"] = "drive"
            self._list_params = params
        return self._list_params

    @gdrive_retry
    def gdrive_upload_file(
        self,
        parent_id,
        title,
        no_progress_bar=True,
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
        if parent_id == self.remote_root_id:
            cached = self.cached_dirs.get(remote_path, [])
            if cached:
                return cached[0]

        item = self._create_remote_dir(parent_id, title)

        if parent_id == self.remote_root_id:
            self.cached_dirs.setdefault(remote_path, []).append(item["id"])
            self.cached_ids[item["id"]] = item["title"]

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
        if self._cache_initialized:
            if path == self.path_info.path:
                return [self.remote_root_id]
            return self.cached_dirs.get(path, [])
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

    def all(self):
        if not self.cached_ids:
            return

        query = "({})".format(
            " or ".join(
                "'{}' in parents".format(dir_id) for dir_id in self.cached_ids
            )
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

    def remove(self, path_info):
        remote_id = self._get_remote_id(path_info)
        self._delete_remote_file(remote_id)
