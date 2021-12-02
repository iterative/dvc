import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.scheme import Schemes
from dvc.utils import format_link, tmp_fname

from .fsspec_wrapper import FSSpecWrapper

logger = logging.getLogger(__name__)
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


class GDriveAuthError(DvcException):
    def __init__(self, cred_location):

        if cred_location:
            message = (
                "GDrive remote auth failed with credentials in '{}'.\n"
                "Backup first, remove or fix them, and run DVC again.\n"
                "It should do auth again and refresh the credentials.\n\n"
                "Details:".format(cred_location)
            )
        else:
            message = "Failed to authenticate GDrive remote"

        super().__init__(message)


class GDriveFileSystem(FSSpecWrapper):  # pylint:disable=abstract-method
    scheme = Schemes.GDRIVE
    PARAM_CHECKSUM = "checksum"
    REQUIRES = {"pydrive2": "pydrive2"}
    # Always prefer traverse for GDrive since API usage quotas are a concern.
    TRAVERSE_WEIGHT_MULTIPLIER = 1
    TRAVERSE_PREFIX_LEN = 2

    GDRIVE_CREDENTIALS_DATA = "GDRIVE_CREDENTIALS_DATA"
    DEFAULT_USER_CREDENTIALS_FILE = "gdrive-user-credentials.json"

    DEFAULT_GDRIVE_CLIENT_ID = "710796635688-iivsgbgsb6uv1fap6635dhvuei09o66c.apps.googleusercontent.com"  # noqa: E501
    DEFAULT_GDRIVE_CLIENT_SECRET = "a1Fz59uTpVNeG_VGuSKDLJXv"

    def __init__(self, **config):
        from fsspec.utils import infer_storage_options

        super().__init__(**config)

        self.url = config["url"]
        opts = infer_storage_options(self.url)

        if not opts["host"]:
            raise DvcException(
                "Empty GDrive URL '{}'. Learn more at {}".format(
                    config["url"],
                    format_link("https://man.dvc.org/remote/add"),
                )
            )

        self._bucket = opts["host"]
        self._path = opts["path"].lstrip("/")
        self._trash_only = config.get("gdrive_trash_only")
        self._use_service_account = config.get("gdrive_use_service_account")
        self._service_account_user_email = config.get(
            "gdrive_service_account_user_email"
        )
        self._service_account_json_file_path = config.get(
            "gdrive_service_account_json_file_path"
        )
        self._client_id = config.get("gdrive_client_id")
        self._client_secret = config.get("gdrive_client_secret")
        self._validate_config()

        tmp_dir = config["gdrive_credentials_tmp_dir"]
        assert tmp_dir

        self._gdrive_service_credentials_path = tmp_fname(
            os.path.join(tmp_dir, "")
        )
        self._gdrive_user_credentials_path = (
            tmp_fname(os.path.join(tmp_dir, ""))
            if os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA)
            else config.get(
                "gdrive_user_credentials_file",
                os.path.join(tmp_dir, self.DEFAULT_USER_CREDENTIALS_FILE),
            )
        )

    @classmethod
    def _strip_protocol(cls, path):
        from fsspec.utils import infer_storage_options

        opts = infer_storage_options(path)

        return "{host}{path}".format(**opts)

    def unstrip_protocol(self, path):
        return f"gdrive://{path}"

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        return {"url": urlpath}

    def _validate_config(self):
        # Validate Service Account configuration
        if (
            self._use_service_account
            and not self._service_account_json_file_path
        ):
            raise DvcException(
                "To use service account, set "
                "`gdrive_service_account_json_file_path`, and optionally"
                "`gdrive_service_account_user_email` in DVC config\n"
                "Learn more at {}".format(
                    format_link("https://man.dvc.org/remote/modify")
                )
            )

        # Validate OAuth 2.0 Client ID configuration
        if not self._use_service_account:
            if bool(self._client_id) != bool(self._client_secret):
                raise DvcException(
                    "Please specify GDrive's client ID and secret in "
                    "DVC config or omit both to use the defaults.\n"
                    "Learn more at {}".format(
                        format_link("https://man.dvc.org/remote/modify")
                    )
                )

    @cached_property
    def credentials_location(self):
        """
        Helper to determine where will GDrive remote read credentials from.
        Useful for tests, exception messages, etc. Returns either env variable
        name if it's set or actual path to the credentials file.
        """
        if os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            return GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
        if os.path.exists(self._gdrive_user_credentials_path):
            return self._gdrive_user_credentials_path
        return None

    @staticmethod
    def _validate_credentials(auth, settings):
        """
        Detects discrepancy in DVC config and cached credentials file.
        Usually happens when a second remote is added and it is using
        the same credentials default file. Or when someones decides to change
        DVC config client id or secret but forgets to remove the cached
        credentials file.
        """
        if not os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA):
            if (
                settings["client_config"]["client_id"]
                != auth.credentials.client_id
                or settings["client_config"]["client_secret"]
                != auth.credentials.client_secret
            ):
                logger.warning(
                    "Client ID and secret configured do not match the "
                    "actual ones used\nto access the remote. Do you "
                    "use multiple GDrive remotes and forgot to\nset "
                    "`gdrive_user_credentials_file` for one or more of them? "
                    "Learn more at\n{}.\n".format(
                        format_link("https://man.dvc.org/remote/modify")
                    )
                )

    @wrap_prop(threading.RLock())
    @cached_property
    def fs(self):
        from pydrive2.auth import GoogleAuth
        from pydrive2.fs import GDriveFileSystem as _GDriveFileSystem

        temporary_save_path = self._gdrive_user_credentials_path
        is_credentials_temp = os.getenv(
            GDriveFileSystem.GDRIVE_CREDENTIALS_DATA
        )
        if self._use_service_account:
            temporary_save_path = self._gdrive_service_credentials_path

        if is_credentials_temp:
            with open(temporary_save_path, "w", encoding="utf-8") as cred_file:
                cred_file.write(
                    os.getenv(GDriveFileSystem.GDRIVE_CREDENTIALS_DATA)
                )

        auth_settings = {
            "client_config_backend": "settings",
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": self._gdrive_user_credentials_path,
            "get_refresh_token": True,
            "oauth_scope": [
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/drive.appdata",
            ],
        }

        if self._use_service_account:
            auth_settings["service_config"] = {
                "client_user_email": self._service_account_user_email,
                "client_json_file_path": self._service_account_json_file_path,
            }
            if is_credentials_temp:
                auth_settings["service_config"][
                    "client_json_file_path"
                ] = temporary_save_path

        else:
            auth_settings["client_config"] = {
                "client_id": self._client_id or self.DEFAULT_GDRIVE_CLIENT_ID,
                "client_secret": self._client_secret
                or self.DEFAULT_GDRIVE_CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "revoke_uri": "https://oauth2.googleapis.com/revoke",
                "redirect_uri": "",
            }

        GoogleAuth.DEFAULT_SETTINGS.update(auth_settings)

        # Pass non existent settings path to force DEFAULT_SETTINGS loadings
        gauth = GoogleAuth(settings_file="")

        try:
            logger.debug(
                "GDrive remote auth with config '{}'.".format(
                    GoogleAuth.DEFAULT_SETTINGS
                )
            )
            if self._use_service_account:
                gauth.ServiceAuth()
            else:
                gauth.CommandLineAuth()
                GDriveFileSystem._validate_credentials(gauth, auth_settings)

        # Handle AuthenticationError, RefreshError and other auth failures
        # It's hard to come up with a narrow exception, since PyDrive throws
        # a lot of different errors - broken credentials file, refresh token
        # expired, flow failed, etc.
        except Exception as exc:
            raise GDriveAuthError(self.credentials_location) from exc
        finally:
            if is_credentials_temp:
                os.remove(temporary_save_path)

        return _GDriveFileSystem(
            f"{self._bucket}/{self._path}",
            gauth,
            trash_only=self._trash_only,
        )

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(os.path.dirname(to_info))
        return self.fs.upload_fobj(fobj, to_info, **kwargs)
