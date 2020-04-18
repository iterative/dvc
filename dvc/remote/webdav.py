from .http import RemoteHTTP
from dvc.scheme import Schemes

import os.path

from dvc.progress import Tqdm
from dvc.exceptions import HTTPError


class RemoteWEBDAV(RemoteHTTP):
    scheme = Schemes.WEBDAV

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        if url:
            self.path_info = self.path_cls(url)
            self.path_info.scheme = self.path_info.scheme.replace(
                "webdav", "http")
            user = config.get("user", None)
            if user:
                self.path_info.user = user
        else:
            self.path_info = None

        self.auth = config.get("auth", None)
        self.custom_auth_header = config.get("custom_auth_header", None)
        self.password = config.get("password", None)
        self.ask_password = config.get("ask_password", False)
        self.headers = {}

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        def chunks():
            with open(from_file, "rb") as fd:
                with Tqdm.wrapattr(
                    fd,
                    "read",
                    total=None
                    if no_progress_bar
                    else os.path.getsize(from_file),
                    leave=False,
                    desc=to_info.url if name is None else name,
                    disable=no_progress_bar,
                ) as fd_wrapped:
                    while True:
                        chunk = fd_wrapped.read(self.CHUNK_SIZE)
                        if not chunk:
                            break
                        yield chunk

        response = self._request("PUT", to_info.url, data=chunks())
        if response.status_code not in (200, 201):
            raise HTTPError(response.status_code, response.reason)
