import logging
import threading

from funcy import cached_property, wrap_prop

from dvc.config import Config
from dvc.config import ConfigError
from dvc.exceptions import DvcException, HTTPError
from dvc.progress import Tqdm
from dvc.remote.base import RemoteBASE
from dvc.scheme import Schemes

logger = logging.getLogger(__name__)


class RemoteHTTP(RemoteBASE):
    scheme = Schemes.HTTP
    SESSION_RETRIES = 5
    SESSION_BACKOFF_FACTOR = 0.1
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 2 ** 16
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL)
        self.path_info = self.path_cls(url) if url else None

        if not self.no_traverse:
            raise ConfigError(
                "HTTP doesn't support traversing the remote to list existing "
                "files. Use: `dvc remote modify <name> no_traverse true`"
            )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        response = self._request("GET", from_info.url, stream=True)
        if response.status_code != 200:
            raise HTTPError(response.status_code, response.reason)
        with Tqdm(
            total=None if no_progress_bar else self._content_length(response),
            leave=False,
            bytes=True,
            desc=from_info.url if name is None else name,
            disable=no_progress_bar,
        ) as pbar:
            with open(to_file, "wb") as fd:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    fd.write(chunk)
                    pbar.update(len(chunk))

    def exists(self, path_info):
        return bool(self._request("HEAD", path_info.url))

    def _content_length(self, response):
        res = response.headers.get("Content-Length")
        return int(res) if res else None

    def get_file_checksum(self, path_info):
        url = path_info.url
        headers = self._request("HEAD", url).headers
        etag = headers.get("ETag") or headers.get("Content-MD5")

        if not etag:
            raise DvcException(
                "could not find an ETag or "
                "Content-MD5 header for '{url}'".format(url=url)
            )

        if etag.startswith("W/"):
            raise DvcException(
                "Weak ETags are not supported."
                " (Etag: '{etag}', URL: '{url}')".format(etag=etag, url=url)
            )

        return etag

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

    def _request(self, method, url, **kwargs):
        import requests

        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            res = self._session.request(method, url, **kwargs)

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
            raise DvcException("could not perform a {} request".format(method))

    def gc(self):
        raise NotImplementedError
