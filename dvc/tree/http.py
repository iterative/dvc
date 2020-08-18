import logging
import os.path
import threading

from funcy import cached_property, memoize, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.exceptions import DvcException, HTTPError
from dvc.path_info import HTTPURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_password(host, user):
    return prompt.password(
        "Enter a password for "
        "host '{host}' user '{user}'".format(host=host, user=user)
    )


class HTTPTree(BaseTree):  # pylint:disable=abstract-method
    scheme = Schemes.HTTP
    PATH_CLS = HTTPURLInfo
    PARAM_CHECKSUM = "etag"
    CAN_TRAVERSE = False

    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 2 ** 16

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        if url:
            self.path_info = self.PATH_CLS(url)
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

    def _auth_method(self, path_info=None):
        from requests.auth import HTTPBasicAuth, HTTPDigestAuth

        if path_info is None:
            path_info = self.path_info

        if self.auth:
            if self.ask_password and self.password is None:
                host, user = path_info.host, path_info.user
                self.password = ask_password(host, user)
            if self.auth == "basic":
                return HTTPBasicAuth(path_info.user, self.password)
            if self.auth == "digest":
                return HTTPDigestAuth(path_info.user, self.password)
            if self.auth == "custom" and self.custom_auth_header:
                self.headers.update({self.custom_auth_header: self.password})
        return None

    @wrap_prop(threading.Lock())
    @cached_property
    def _session(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        retries = Retry(
            total=self.SESSION_RETRIES,
            backoff_factor=self.SESSION_BACKOFF_FACTOR,
        )

        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session

    def request(self, method, url, **kwargs):
        import requests

        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            res = self._session.request(
                method,
                url,
                auth=self._auth_method(),
                headers=self.headers,
                **kwargs,
            )

            redirect_no_location = (
                kwargs["allow_redirects"]
                and res.status_code in (301, 302)
                and "location" not in res.headers
            )

            if redirect_no_location:
                # AWS s3 doesn't like to add a location header to its redirects
                # from https://s3.amazonaws.com/<bucket name>/* type URLs.
                # This should be treated as an error
                raise requests.exceptions.RequestException

            return res

        except requests.exceptions.RequestException:
            raise DvcException(f"could not perform a {method} request")

    def _head(self, url):
        response = self.request("HEAD", url)
        if response.ok:
            return response

        # Sometimes servers are configured to forbid HEAD requests
        # Context: https://github.com/iterative/dvc/issues/4131
        with self.request("GET", url, stream=True) as r:
            if r.ok:
                return r

        return response

    def exists(self, path_info, use_dvcignore=True):
        return bool(self._head(path_info.url))

    def get_file_hash(self, path_info):
        url = path_info.url

        headers = self._head(url).headers

        etag = headers.get("ETag") or headers.get("Content-MD5")

        if not etag:
            raise DvcException(
                "could not find an ETag or "
                "Content-MD5 header for '{url}'".format(url=url)
            )

        return self.PARAM_CHECKSUM, etag

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        response = self.request("GET", from_info.url, stream=True)
        if response.status_code != 200:
            raise HTTPError(response.status_code, response.reason)
        with open(to_file, "wb") as fd:
            with Tqdm.wrapattr(
                fd,
                "write",
                total=None
                if no_progress_bar
                else self._content_length(response),
                leave=False,
                desc=from_info.url if name is None else name,
                disable=no_progress_bar,
            ) as fd_wrapped:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    fd_wrapped.write(chunk)

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

        response = self.request("POST", to_info.url, data=chunks())
        if response.status_code not in (200, 201):
            raise HTTPError(response.status_code, response.reason)

    @staticmethod
    def _content_length(response):
        res = response.headers.get("Content-Length")
        return int(res) if res else None
