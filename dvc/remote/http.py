from __future__ import unicode_literals

from dvc.scheme import Schemes
from dvc.path import Path
from dvc.utils.compat import open, makedirs

import os
import threading
import requests
import logging

from dvc.progress import progress
from dvc.exceptions import DvcException
from dvc.config import Config
from dvc.remote.base import RemoteBASE
from dvc.utils import move


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
    REGEX = r"^http://.*$"
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 2 ** 16
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteHTTP, self).__init__(repo, config)
        self.cache_dir = config.get(Config.SECTION_REMOTE_URL)
        self.url = self.cache_dir

        self.path_info = Path(self.scheme)

    @property
    def prefix(self):
        return self.cache_dir

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(to_infos, from_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != self.scheme:
                raise NotImplementedError

            if to_info.scheme != "local":
                raise NotImplementedError

            msg = "Downloading '{}' to '{}'".format(
                from_info.path, to_info.path
            )
            logger.debug(msg)

            if not name:
                name = os.path.basename(to_info.path)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)

            total = self._content_length(from_info.path)

            if no_progress_bar or not total:
                cb = None
            else:
                cb = ProgressBarCallback(name, total)

            try:
                self._download_to(
                    from_info.path, to_info.path, callback=cb, resume=resume
                )

            except Exception:
                msg = "failed to download '{}'".format(from_info.path)
                logger.exception(msg)
                continue

            if not no_progress_bar:
                progress.finish_target(name)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == self.scheme
        return bool(self._request("HEAD", path_info.path))

    def cache_exists(self, md5s):
        assert isinstance(md5s, list)

        def func(md5):
            return bool(self._request("HEAD", self.checksum_to_path(md5)))

        return list(filter(func, md5s))

    def _content_length(self, url):
        return self._request("HEAD", url).headers.get("Content-Length")

    def get_file_checksum(self, path_info):
        url = path_info.path
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

    def _download_to(self, url, target_file, callback=None, resume=False):
        request = self._request("GET", url, stream=True)
        partial_file = target_file + ".part"

        mode, transferred_bytes = self._determine_mode_get_transferred_bytes(
            partial_file, resume
        )

        self._validate_existing_file_size(transferred_bytes, partial_file)

        self._write_request_content(
            mode, partial_file, request, transferred_bytes, callback
        )

        move(partial_file, target_file)

    def _write_request_content(
        self, mode, partial_file, request, transferred_bytes, callback=None
    ):
        with open(partial_file, mode) as fd:

            for index, chunk in enumerate(
                request.iter_content(chunk_size=self.CHUNK_SIZE)
            ):
                chunk_number = index + 1
                if chunk_number * self.CHUNK_SIZE > transferred_bytes:
                    fd.write(chunk)
                    fd.flush()
                    transferred_bytes += len(chunk)

                if callback:
                    callback(transferred_bytes)

    def _validate_existing_file_size(self, bytes_transferred, partial_file):
        if bytes_transferred % self.CHUNK_SIZE != 0:
            raise DvcException(
                "File {}, might be corrupted, please remove "
                "it and retry importing".format(partial_file)
            )

    def _determine_mode_get_transferred_bytes(self, partial_file, resume):
        if os.path.exists(partial_file) and resume:
            mode = "ab"
            bytes_transfered = os.path.getsize(partial_file)
        else:
            mode = "wb"
            bytes_transfered = 0
        return mode, bytes_transfered

    def _request(self, method, url, **kwargs):
        kwargs.setdefault("allow_redirects", True)
        kwargs.setdefault("timeout", self.REQUEST_TIMEOUT)

        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException:
            raise DvcException("could not perform a {} request".format(method))

    def gc(self):
        raise NotImplementedError
