from hashlib import md5
import datetime
import json
import os

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession
import google.oauth2.credentials

from dvc.config import Config
from dvc.remote.gdrive.waitable_lock import WaitableLock


class OAuth2(object):

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

    def __init__(self, oauth_id, credentialpath, scopes, flow_runner):
        self.oauth_id = oauth_id
        self.credentialpath = credentialpath
        self.scopes = scopes
        self.flow_runner = flow_runner

    def get_session(self):
        creds_storage, creds_storage_lock = self._get_storage_lock()
        with creds_storage_lock:
            if os.path.exists(creds_storage):
                creds = self._load_credentials(creds_storage)
            else:
                creds = self._acquire_credentials()
                self._save_credentials(creds_storage, creds)
        return AuthorizedSession(creds)

    def _get_creds_id(self, client_id):
        plain_text = "|".join([self.oauth_id, client_id] + self.scopes)
        hashed = md5(plain_text.encode("ascii")).hexdigest()
        return hashed

    def _acquire_credentials(self):
        # Create the flow using the client secrets file from the
        # Google API Console.
        flow = InstalledAppFlow.from_client_secrets_file(
            self.credentialpath, scopes=self.scopes
        )
        if self.flow_runner == "local":
            creds = flow.run_local_server()
        elif self.flow_runner == "console":
            creds = flow.run_console()
        else:
            raise ValueError(
                "oauth2 flow runner should be 'local' or 'console'"
            )
        return creds

    def _load_credentials(self, creds_storage):
        """Load credentials from json file and refresh them if needed

        Should be called under lock.
        """
        info = json.load(open(creds_storage))
        creds = google.oauth2.credentials.Credentials(
            token=info["token"],
            refresh_token=info["refresh_token"],
            token_uri=info["token_uri"],
            client_id=info["client_id"],
            client_secret=info["client_secret"],
            scopes=self.scopes,
        )
        creds.expiry = datetime.datetime.strptime(
            info["expiry"], self.DATETIME_FORMAT
        )
        if creds.expired:
            creds.refresh(google.auth.transport.requests.Request())
            self._save_credentials(creds_storage, creds)
        return creds

    def _save_credentials(self, creds_storage, creds):
        """Save credentials to the json file

        Should be called under lock.
        """
        with open(creds_storage, "w") as f:
            info = {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": creds.scopes,
                "expiry": creds.expiry.strftime(self.DATETIME_FORMAT),
            }
            json.dump(info, f)

    def get_storage_filename(self):
        creds_storage_dir = os.path.join(
            Config.get_global_config_dir(), "gdrive-oauth2"
        )
        if not os.path.exists(creds_storage_dir):
            os.makedirs(creds_storage_dir)
        info = json.load(open(self.credentialpath))
        creds_id = self._get_creds_id(info["installed"]["client_id"])
        return os.path.join(creds_storage_dir, creds_id)

    def _get_storage_lock(self):
        creds_storage = self.get_storage_filename()
        # 5 minutes timeout is needed to allow the user to get the
        # token when accessing the remote first time
        timeout = 5 * 60
        return (
            creds_storage,
            WaitableLock(creds_storage + ".lock", timeout=timeout),
        )
