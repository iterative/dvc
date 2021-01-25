import logging
import os.path
import threading

from funcy import cached_property, memoize, wrap_prop, wrap_with

import dvc.prompt as prompt
from dvc.exceptions import DvcException, HTTPError
from dvc.hash_info import HashInfo
from dvc.path_info import HTTPURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


@wrap_with(threading.Lock())
@memoize
def ask_username(host):
    return prompt.username(
        "Username for `{host}`".format(host=host)
    )


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
    REQUEST_TIMEOUT = 60
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
        self.ssl_verify = config.get("ssl_verify", True)
        self.method = config.get("method", "POST")

    @memoize
    def _get_user(self, host):
        user = ask_username(host)
        self.path_info.user = user
        with self.repo.config.edit("local") as conf:
            section = conf["remote"][self.config["remote_name"]]
            section["user"] = user
            self.repo.config["user"] = user
        return user

    @memoize
    def _get_password(self, host, user):
        import keyring
        password = keyring.get_password(host, user)
        if password is None or self.ask_password:
            password = ask_password(host, user)
            keyring.set_password(host, user, password)
        return password

    @wrap_prop(threading.Lock())
    def _auth_method(self, path_info=None):
        from requests.auth import HTTPBasicAuth, HTTPDigestAuth

        if path_info is None:
            path_info = self.path_info

        if self.auth:
            if self.auth == "basic":
                host = path_info.host
                user = path_info.user
                if user is None:
                    user = self._get_user(host)
                password = self._get_password(user, host)
                return HTTPBasicAuth(user, password)
            if self.auth == "digest":
                return HTTPDigestAuth(path_info.user, self.password)
            if self.auth == "custom" and self.custom_auth_header:
                self.headers.update({self.custom_auth_header: self.password})
        return None

    def _generate_download_url(self, path_info):
        return path_info.url

    @wrap_prop(threading.Lock())
    @cached_property
    def _session(self):
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        session = requests.Session()

        session.verify = self.ssl_verify

        retries = Retry(
            total=self.SESSION_RETRIES,
            backoff_factor=self.SESSION_BACKOFF_FACTOR,
        )

        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))

        return session

    def request(self, method, url, **kwargs):
        auth_method = self._auth_method()
        res = self._request(method, url, auth_method, **kwargs)
        if auth_method is None and res.status_code == 401:
            res_auth_method = res.headers.get('WWW-Authenticate')
            if res_auth_method is not None and res_auth_method.lower().startswith("basic "):
                self.auth = "basic"
            auth_method = self._auth_method()
            res = self._request(method, url, auth_method, **kwargs)
        return res

    def _request(self, method, url, auth_method, **kwargs):
        import requests

        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            res = self._session.request(
                method,
                url,
                auth=auth_method,
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
        res = self._head(path_info.url)
        if res.status_code == 404:
            return False
        if bool(res):
            return True
        raise HTTPError(res.status_code, res.reason)

    def getsize(self, path_info):
        response = self.request("GET", path_info.url, stream=True)
        if response.status_code != 200:
            raise HTTPError(response.status_code, response.reason)
        return self._content_length(response)

    def get_file_hash(self, path_info):
        url = path_info.url

        headers = self._head(url).headers

        etag = headers.get("ETag") or headers.get("Content-MD5")

        if not etag:
            raise DvcException(
                "could not find an ETag or "
                "Content-MD5 header for '{url}'".format(url=url)
            )

        return HashInfo(self.PARAM_CHECKSUM, etag)

    def _upload_fobj(self, fobj, to_info):
        def chunks(fobj):
            while True:
                chunk = fobj.read(self.CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

        response = self.request(self.method, to_info.url, data=chunks(fobj))
        if response.status_code not in (200, 201):
            raise HTTPError(response.status_code, response.reason)

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

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                no_progress_bar=no_progress_bar,
                desc=name or to_info.url,
                total=None if no_progress_bar else os.path.getsize(from_file),
            )

    @staticmethod
    def _content_length(response):
        res = response.headers.get("Content-Length")
        return int(res) if res else None
