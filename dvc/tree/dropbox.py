import datetime
import json
import logging
import os
import threading
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from contextlib import closing

from funcy import cached_property, retry, wrap_prop

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.utils import format_link

from .base import BaseTree

logger = logging.getLogger(__name__)


def _dropbox_retry(func):
    def should_retry(exc):
        import dropbox

        if not isinstance(
            exc,
            (
                dropbox.exceptions.InternalServerError,
                dropbox.exceptions.RateLimitError,
            ),
        ):
            return False

        logger.debug(f"Retrying Dropbox API call, error: {exc}.")

        return True

    # 16 tries, start at 0.5s, multiply by golden ratio, cap at 20s
    return retry(
        16,
        timeout=lambda a: min(0.5 * 1.618 ** a, 20),
        filter_errors=should_retry,
    )(func)


class DropboxWrapper:
    def __init__(self, client):
        self.client = client

    @_dropbox_retry
    def files_get_temporary_link(self, *args, **kwargs):
        return self.client.files_get_temporary_link(*args, **kwargs)

    @_dropbox_retry
    def files_get_metadata(self, *args, **kwargs):
        return self.client.files_get_metadata(*args, **kwargs)

    @_dropbox_retry
    def files_list_folder(self, *args, **kwargs):
        return self.client.files_list_folder(*args, **kwargs)

    @_dropbox_retry
    def files_list_folder_continue(self, *args, **kwargs):
        return self.client.files_list_folder_continue(*args, **kwargs)

    @_dropbox_retry
    def files_delete_v2(self, *args, **kwargs):
        return self.client.files_delete_v2(*args, **kwargs)

    @_dropbox_retry
    def files_upload(self, *args, **kwargs):
        return self.client.files_upload(*args, **kwargs)

    @_dropbox_retry
    def files_upload_session_start(self, *args, **kwargs):
        return self.client.files_upload_session_start(*args, **kwargs)

    @_dropbox_retry
    def files_upload_session_append_v2(self, *args, **kwargs):
        return self.client.files_upload_session_append_v2(*args, **kwargs)

    @_dropbox_retry
    def files_upload_session_finish(self, *args, **kwargs):
        return self.client.files_upload_session_finish(*args, **kwargs)

    @_dropbox_retry
    def files_download(self, *args, **kwargs):
        return self.client.files_download(*args, **kwargs)


ACCESS_TOKEN = "DROPBOX_ACCESS_TOKEN"
EXPIRATION = "DROPBOX_EXPIRES_AT"
REFRESH_TOKEN = "DROPBOX_REFRESH_TOKEN"


Auth = namedtuple("Auth", ["client", "creds"])


class DropboxCredProvider(metaclass=ABCMeta):
    DROPBOX_APP_KEY = ""
    DROPBOX_APP_SECRET = ""

    @abstractmethod
    def can_auth(self):
        raise NotImplementedError

    @abstractmethod
    def _gather_auth(self):
        raise NotImplementedError

    def auth(self):
        import dropbox

        config = self._gather_auth()
        if config[EXPIRATION]:
            config[EXPIRATION] = datetime.datetime.fromisoformat(
                config[EXPIRATION]
            )
        else:
            config[EXPIRATION] = datetime.datetime.now()
        dbx = dropbox.Dropbox(
            oauth2_access_token=config[ACCESS_TOKEN],
            oauth2_access_token_expiration=config[EXPIRATION],
            oauth2_refresh_token=config[REFRESH_TOKEN],
            app_key=self.DROPBOX_APP_KEY,
            app_secret=self.DROPBOX_APP_SECRET,
        )
        dbx.check_and_refresh_access_token()
        try:
            dbx.check_user()
        except dropbox.exceptions.AuthError as ex:
            raise DvcException(
                "To use Dropbox account, you need to login.\n"
                "If you already did, your credentials may be outdated.\n"
                "Learn more at {}".format(
                    format_link("https://man.dvc.org/remote/modify")
                )
            ) from ex
        return Auth(DropboxWrapper(dbx), config)

    @abstractmethod
    def save(self, creds):
        raise NotImplementedError


class CliCredProvider(DropboxCredProvider):
    def can_auth(self):
        return True

    def _gather_auth(self):
        import dropbox

        flow = dropbox.DropboxOAuth2FlowNoRedirect(
            consumer_key=self.DROPBOX_APP_KEY,
            consumer_secret=self.DROPBOX_APP_SECRET,
            token_access_type="offline",
        )
        authorize_url = flow.start()
        print("Go to the following link in your browser:")
        print()
        print("    " + authorize_url)
        print()
        code = input("Enter verification code: ").strip()
        try:
            auth = flow.finish(code)
        except dropbox.oauth.NotApprovedException as ex:
            raise DvcException(
                "To use Dropbox remote, you need to approve the DVC "
                "application to access your Dropbox account.\n"
                "Learn more at {}".format(
                    format_link("https://man.dvc.org/remote/modify")
                )
            ) from ex
        return {
            ACCESS_TOKEN: auth.access_token,
            EXPIRATION: auth.expires_at.isoformat(),
            REFRESH_TOKEN: auth.refresh_token,
        }

    def save(self, creds):
        pass


class EnvCredProvider(DropboxCredProvider):
    def can_auth(self):
        return bool(os.environ.get(REFRESH_TOKEN, False))

    def _gather_auth(self):
        return {
            ACCESS_TOKEN: os.environ.get(ACCESS_TOKEN, ""),
            EXPIRATION: os.environ.get(EXPIRATION, ""),
            REFRESH_TOKEN: os.environ[REFRESH_TOKEN],
        }

    def save(self, creds):
        _creds = creds.copy()
        _creds[EXPIRATION] = _creds[EXPIRATION].isoformat()
        os.environ.update(_creds)


class FileCredProvider(DropboxCredProvider):
    DEFAULT_FILE = "dropbox-user-credentials.json"
    DROPBOX_CREDENTIALS_FILE = "DROPBOX_CREDENTIALS_FILE"

    def __init__(self, repo):
        self._cred_location = os.environ.get(
            self.DROPBOX_CREDENTIALS_FILE,
            os.path.join(repo.tmp_dir, self.DEFAULT_FILE),
        )

    def can_auth(self):
        if not os.path.exists(self._cred_location):
            return False
        with open(self._cred_location) as infile:
            return REFRESH_TOKEN in json.load(infile)

    def _gather_auth(self):
        with open(self._cred_location) as infile:
            config = json.load(infile)
        return {
            ACCESS_TOKEN: config.get(ACCESS_TOKEN, ""),
            EXPIRATION: config.get(EXPIRATION, ""),
            REFRESH_TOKEN: config[REFRESH_TOKEN],
        }

    def save(self, creds):
        _creds = creds.copy()
        _creds[EXPIRATION] = _creds[EXPIRATION].isoformat()
        with open(self._cred_location, "w") as outfile:
            json.dump(_creds, outfile)


def path_info_to_dropbox_path(path_info):
    return "/" + path_info.bucket + "/" + path_info.path


class DropboxTree(BaseTree):
    scheme = Schemes.DROPBOX
    PATH_CLS = CloudURLInfo
    REQUIRES = {
        "dropbox": "dropbox",
    }
    PARAM_CHECKSUM = "content_hash"
    # Per Dropbox API docs:
    # This is an approximate number and there can be slightly more entries
    # returned in some cases.
    LIST_OBJECT_PAGE_SIZE = 1000
    # Always prefer traverse for Dropbox since API usage quotas are a concern.
    TRAVERSE_WEIGHT_MULTIPLIER = 1
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, repo, config):
        super().__init__(repo, config)

        self.path_info = self.PATH_CLS(config.get("url", "dropbox://default"))
        self.chunk_size_mb = config.get("chunk_size", 48)

        if self.chunk_size_mb % 4 != 0 or not 4 <= self.chunk_size_mb <= 150:
            raise DvcException(
                "Invalid chunk size for Dropbox client.\n"
                "Chunk size must be between 4 and 150 MB and a multiple of 4."
                " Was: {}".format(self.chunk_size_mb)
            )

    @wrap_prop(threading.Lock())
    @cached_property
    def client(self):
        cred_prv = EnvCredProvider()
        if cred_prv.can_auth():
            logger.debug("Logging into Dropbox with environment variables")
            return cred_prv.auth().client
        cred_prv = FileCredProvider(self.repo)
        if cred_prv.can_auth():
            logger.debug("Logging into Dropbox with credentials file")
            return cred_prv.auth().client
        logger.debug("Logging into Dropbox with CLI")
        auth = CliCredProvider().auth()
        logger.debug("Saving Dropbox credentials to credentials file")
        cred_prv.save(auth.creds)
        return auth.client

    def _generate_download_url(self, path_info):
        import dropbox

        path = path_info_to_dropbox_path(path_info)
        try:
            # expires in 4 hrs
            return self.client.files_get_temporary_link(path).link
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path() and ex.error.get_path().is_not_found():
                raise DvcException(
                    "Path not found for '{}':\n\n"
                    "1. Confirm the file exists and you can access it.\n"
                    "2. Make sure that credentials in '{}'\n"
                    "   are correct for this remote e.g. "
                    "use the `dropbox_user_credentials_file` config\n"
                    "   option if you use multiple Dropbox remotes with "
                    "different email accounts.\n\nDetails".format(
                        path, FileCredProvider.DEFAULT_FILE
                    )
                ) from ex
            raise

    def exists(self, path_info, use_dvcignore=True):
        import dropbox

        path = path_info_to_dropbox_path(path_info)
        logger.debug("Checking existence of {0}".format(path))
        try:
            self.client.files_get_metadata(path)
            return True
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path() and ex.error.get_path().is_not_found():
                return False
            raise

    def walk_files(self, path_info, **kwargs):
        """Return a generator with `PathInfo`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path_info` will be treated as a prefix
                rather than directory path.
        """
        import dropbox
        from dropbox.files import FileMetadata

        path = path_info_to_dropbox_path(path_info)
        if not kwargs.pop("prefix", False):
            path = path + "/"
        try:
            res = self.client.files_list_folder(
                path, recursive=True, limit=self.LIST_OBJECT_PAGE_SIZE
            )
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path() and ex.error.get_path().is_not_found():
                return
            raise
        while True:
            for entry in res.entries:
                if isinstance(entry, FileMetadata):
                    post_bucket = entry.path_lower[len(path_info.bucket) + 1 :]
                    replaced = path_info.replace(post_bucket)
                    yield replaced
            if not res.has_more:
                break
            res = self.client.files_list_folder_continue(res.cursor)

    def remove(self, path_info):
        import dropbox

        path = path_info_to_dropbox_path(path_info)
        logger.debug("Removing {0}".format(path))
        try:
            self.client.files_delete_v2(path)
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path_lookup() or ex.error.is_path_write():
                raise DvcException(
                    "No write access for '{}':\n\n"
                    "1. Confirm the file exists and you can write it.\n"
                    "2. Make sure that credentials in '{}'\n"
                    "   are correct for this remote e.g. "
                    "use the `dropbox_user_credentials_file` config\n"
                    "   option if you use multiple Dropbox remotes with "
                    "different email accounts.\n\nDetails".format(
                        path, FileCredProvider.DEFAULT_FILE
                    )
                ) from ex
            raise

    def get_file_hash(self, path_info):
        import dropbox

        path = path_info_to_dropbox_path(path_info)
        logger.debug("Getting hash of {0}".format(path))
        try:
            return HashInfo(
                self.PARAM_CHECKSUM,
                self.client.files_get_metadata(path).content_hash,
            )
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path() and ex.error.get_path().is_not_found():
                raise DvcException(
                    "Path not found for '{}':\n\n"
                    "1. Confirm the file exists and you can access it.\n"
                    "2. Make sure that credentials in '{}'\n"
                    "   are correct for this remote e.g. "
                    "use the `dropbox_user_credentials_file` config\n"
                    "   option if you use multiple Dropbox remotes with "
                    "different email accounts.\n\nDetails".format(
                        path, FileCredProvider.DEFAULT_FILE
                    )
                ) from ex
            raise

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        import dropbox

        chunk_size = self.chunk_size_mb * 1024 * 1024
        to_path = path_info_to_dropbox_path(to_info)
        file_size = os.path.getsize(from_file)
        logger.debug("Uploading " + from_file + " to " + to_path)
        with open(from_file, "rb") as fobj, Tqdm.wrapattr(
            fobj, "read", desc=name, total=file_size, disable=no_progress_bar
        ) as wrapped:
            if file_size <= chunk_size:
                logger.debug("Small file upload")
                self.client.files_upload(wrapped.read(), to_path)
            else:
                logger.debug("Big file upload")
                session = self.client.files_upload_session_start(
                    wrapped.read(chunk_size)
                )
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=session.session_id, offset=wrapped.tell(),
                )
                commit = dropbox.files.CommitInfo(path=to_path, mute=True)
                while wrapped.tell() < file_size:
                    if (file_size - wrapped.tell()) <= chunk_size:
                        self.client.files_upload_session_finish(
                            wrapped.read(chunk_size), cursor, commit
                        )
                    else:
                        self.client.files_upload_session_append_v2(
                            wrapped.read(chunk_size), cursor,
                        )
                        cursor.offset = wrapped.tell()

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        import dropbox

        from_path = path_info_to_dropbox_path(from_info)
        logger.debug("Downloading " + from_path + " to " + to_file)
        try:
            meta, res = self.client.files_download(from_path)
        except dropbox.exceptions.ApiError as ex:
            if ex.error.is_path() and ex.error.get_path().is_not_found():
                raise DvcException(
                    "Path not found for '{}':\n\n"
                    "1. Confirm the file exists and you can access it.\n"
                    "2. Make sure that credentials in '{}'\n"
                    "   are correct for this remote e.g. "
                    "use the `dropbox_user_credentials_file` config\n"
                    "   option if you use multiple Dropbox remotes with "
                    "different email accounts.\n\nDetails".format(
                        from_path, FileCredProvider.DEFAULT_FILE
                    )
                ) from ex
            if ex.error.is_unsupported_file():
                raise DvcException(
                    "Path '{}' is not downloadable:\n\n"
                    "Confirm the file is a casual file, not a Dropbox thing."
                    "\n\nDetails".format(from_path)
                ) from ex
            raise
        with open(to_file, "wb") as fobj, closing(res) as body, Tqdm.wrapattr(
            fobj, "write", desc=name, total=meta.size, disable=no_progress_bar
        ) as wrapped:
            for chunk in body.iter_content():
                wrapped.write(chunk)
