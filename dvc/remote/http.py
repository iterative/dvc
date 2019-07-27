from __future__ import unicode_literals

from dvc.scheme import Schemes

from dvc.utils.compat import open

import threading
import requests
import logging

from dvc.progress import progress
from dvc.exceptions import DvcException
from dvc.config import Config
from dvc.remote.base import RemoteBASE


logger = logging.getLogger(__name__)


class ProgressBarCallback(object):
    def __init__(self, name, total):
        self.name = name
        self.total = total
        self.current = 0
        self.lock = threading.Lock()

    def __call__(self, byts):
        with self.lock:
            self.current += byts
            progress.update_target(self.name, self.current, self.total)


class RemoteHTTP(RemoteBASE):
    scheme = Schemes.HTTP
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 2 ** 16
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteHTTP, self).__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL)
        self.path_info = self.path_cls(url) if url else None

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        callback = None
        if not no_progress_bar:
            total = self._content_length(from_info.url)
            if total:
                callback = ProgressBarCallback(name, total)

        request = self._request("GET", from_info.url, stream=True)

        with open(to_file, "wb") as fd:
            transferred_bytes = 0

            for chunk in request.iter_content(chunk_size=self.CHUNK_SIZE):
                fd.write(chunk)
                fd.flush()
                transferred_bytes += len(chunk)

                if callback:
                    callback(transferred_bytes)

    def exists(self, path_info):
        return bool(self._request("HEAD", path_info.url))

    def batch_exists(self, path_infos, callback):
        results = []

        for path_info in path_infos:
            results.append(self.exists(path_info))
            callback(str(path_info))

        return results

    def _content_length(self, url):
        return self._request("HEAD", url).headers.get("Content-Length")

    def get_file_checksum(self, path_info):
        url = path_info.url
        etag = self._request("HEAD", url).headers.get("ETag") or self._request(
            "HEAD", url
        ).headers.get("Content-MD5")

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

    def _request(self, method, url, **kwargs):
        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException:
            raise DvcException("could not perform a {} request".format(method))

    def gc(self):
        raise NotImplementedError
