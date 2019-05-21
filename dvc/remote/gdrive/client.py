from time import sleep
import logging
import posixpath
import os

from funcy import cached_property

from requests import ConnectionError

from dvc.progress import progress
from dvc.remote.gdrive.utils import (
    metadata_isdir,
    response_is_ratelimit,
    MIME_GOOGLE_APPS_FOLDER,
    response_error_message,
)
from dvc.remote.gdrive.exceptions import (
    GDriveError,
    GDriveHTTPError,
    GDriveResourceNotFound,
)
from dvc.remote.gdrive.oauth2 import OAuth2

logger = logging.getLogger(__name__)


class GDriveClient:

    GOOGLEAPIS_BASE_URL = "https://www.googleapis.com/"
    TIMEOUT = (5, 60)

    def __init__(
        self,
        space,
        oauth_id,
        credentialpath,
        scopes,
        oauth2_flow_runner,
        max_retries=10,
    ):
        self.space = space
        self.oauth_id = oauth_id
        self.credentialpath = credentialpath
        self.scopes = scopes
        self.oauth2_flow_runner = oauth2_flow_runner
        self.max_retries = max_retries
        self.oauth2 = OAuth2(
            oauth_id, credentialpath, scopes, oauth2_flow_runner
        )

    @cached_property
    def session(self):
        """AuthorizedSession to communicate with https://googleapis.com

        Security notice:

        It always adds the Authorization header to the requests, not paying
        attention is request is for googleapis.com or not. It is just how
        AuthorizedSession from google-auth implements adding its headers. Don't
        use RemoteGDrive.session() to send requests to domains other than
        googleapis.com.
        """
        return self.oauth2.get_session()

    def request(self, method, path, *args, **kwargs):
        # Google Drive has tight rate limits, which strikes the
        # performance and gives the 403 and 429 errors.
        # See https://developers.google.com/drive/api/v3/handle-errors
        retries = 0
        exponential_backoff = 1
        if "timeout" not in kwargs:
            kwargs["timeout"] = self.TIMEOUT
        while retries < self.max_retries:
            retries += 1
            response = self.session.request(
                method, self.GOOGLEAPIS_BASE_URL + path, *args, **kwargs
            )
            if response_is_ratelimit(response) or response.status_code >= 500:
                logger.debug(
                    "got {} response, will retry in {} sec".format(
                        response.status_code, exponential_backoff
                    )
                )
                sleep(exponential_backoff)
                exponential_backoff *= 2
            else:
                break
        if response.status_code >= 400:
            raise GDriveHTTPError(response)
        return response

    def search(self, parent=None, name=None, add_params={}):
        query = []
        if parent is not None:
            query.append("'{}' in parents".format(parent))
        if name is not None:
            query.append("name = '{}'".format(name))
        params = {"q": " and ".join(query), "spaces": self.space}
        params.update(add_params)
        while True:
            data = self.request("GET", "drive/v3/files", params=params).json()
            for i in data["files"]:
                yield i
            if not data.get("nextPageToken"):
                break
            params["pageToken"] = data["nextPageToken"]

    def get_metadata(self, path_info, fields=None):
        parent = self.request(
            "GET", "drive/v3/files/" + path_info.netloc
        ).json()
        current_path = ["gdrive://" + path_info.netloc]
        parts = path_info.path.split("/")
        kwargs = [{} for i in parts]
        if fields is not None:
            # only specify fields for the last search query
            kwargs[-1]["add_params"] = {
                "fields": "files({})".format(",".join(fields))
            }
        for part, kwargs in zip(parts, kwargs):
            if not metadata_isdir(parent):
                raise GDriveError(
                    "{} is not a folder".format("/".join(current_path))
                )
            current_path.append(part)
            files = list(self.search(parent["id"], part, **kwargs))
            if len(files) > 1:
                raise GDriveError(
                    "path {} is duplicated".format("/".join(current_path))
                )
            elif len(files) == 0:
                raise GDriveResourceNotFound("/".join(current_path))
            parent = files[0]
        return parent

    def exists(self, path_info):
        try:
            self.get_metadata(path_info, fields=["id"])
            return True
        except GDriveResourceNotFound:
            return False

    def list_children(self, folder_id):
        for i in self.search(parent=folder_id):
            if metadata_isdir(i):
                for j in self.list_children(i["id"]):
                    yield i["name"] + "/" + j
            else:
                yield i["name"]

    def mkdir(self, parent, name):
        data = {
            "name": name,
            "mimeType": MIME_GOOGLE_APPS_FOLDER,
            "parents": [parent],
            "spaces": self.space,
        }
        return self.request("POST", "drive/v3/files", json=data).json()

    def _resumable_upload_initiate(self, parent, filename):
        response = self.request(
            "POST",
            "upload/drive/v3/files",
            params={"uploadType": "resumable"},
            json={"name": filename, "space": self.space, "parents": [parent]},
        )
        return response.headers["Location"]

    def _resumable_upload_first_request(
        self, resumable_upload_url, from_file, to_info, file_size
    ):
        try:
            # outside of self.request() because this process
            # doesn't need it to handle errors and retries,
            # they are handled in the next "while" loop
            response = self.session.put(
                resumable_upload_url,
                data=from_file,
                headers={"Content-Length": str(file_size)},
                timeout=self.TIMEOUT,
            )
            return response.status_code in (200, 201)
        except ConnectionError:
            return False

    def _resumable_upload_resume(
        self, resumable_upload_url, from_file, to_info, file_size
    ):
        # determine the offset
        response = self.session.put(
            resumable_upload_url,
            headers={
                "Content-Length": str(0),
                "Content-Range": "bytes */{}".format(file_size),
            },
            timeout=self.TIMEOUT,
        )
        if response.status_code in (200, 201):
            # file has been already uploaded
            return True
        elif response.status_code == 404:
            # restarting upload from the beginning wouldn't make a
            # profit, so it is better to notify the user
            raise GDriveError("upload failed, try again")
        elif response.status_code != 308:
            logger.error(
                "upload resume failure: {}".format(
                    response_error_message(response)
                )
            )
            return False
        # ^^ response.status_code is 308 (Resume Incomplete) - continue
        # the upload

        if "Range" in response.headers:
            # if Range header contains a string "bytes 0-9/20"
            # then the server has received the bytes from 0 to 9
            # (including the ends), so upload should be resumed from
            # byte 10
            offset = int(response.headers["Range"].split("-")[-1]) + 1
        else:
            # there could be no Range header in the server response,
            # then upload should be resumed from start
            offset = 0
        logger.debug(
            "resuming {} upload from offset {}".format(to_info, offset)
        )

        # resume the upload
        from_file.seek(offset, 0)
        response = self.session.put(
            resumable_upload_url,
            data=from_file,
            headers={
                "Content-Length": str(file_size - offset),
                "Content-Range": "bytes {}-{}/{}".format(
                    offset, file_size - 1, file_size
                ),
            },
            timeout=self.TIMEOUT,
        )
        return response.status_code in (200, 201)

    def upload(self, parent, to_info, from_file):
        # Resumable upload protocol implementation
        # https://developers.google.com/drive/api/v3/manage-uploads#resumable
        resumable_upload_url = self._resumable_upload_initiate(
            parent, posixpath.basename(to_info.path)
        )
        file_size = os.fstat(from_file.fileno()).st_size
        success = self._resumable_upload_first_request(
            resumable_upload_url, from_file, to_info, file_size
        )
        errors_count = 0
        while not success:
            try:
                success = self._resumable_upload_resume(
                    resumable_upload_url, from_file, to_info, file_size
                )
            except ConnectionError:
                errors_count += 1
                if errors_count >= 10:
                    raise
                sleep(1.0)

    def download(self, from_info, to_file, name, no_progress_bar):
        metadata = self.get_metadata(
            from_info, fields=["id", "mimeType", "size"]
        )
        response = self.request(
            "GET",
            "drive/v3/files/" + metadata["id"],
            params={"alt": "media"},
            stream=True,
        )
        current = 0
        if response.status_code != 200:
            raise GDriveHTTPError(response)
        with open(to_file, "wb") as f:
            for chunk in response.iter_content(4096):
                f.write(chunk)
                if not no_progress_bar:
                    current += len(chunk)
                    progress.update_target(name, current, metadata["size"])
