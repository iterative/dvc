from __future__ import unicode_literals

from dvc.scheme import Schemes

from dvc.utils.compat import open, makedirs, fspath_py35

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
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 2 ** 16
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteHTTP, self).__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL)
        self.path_info = self.path_cls(url) if url else None

    def download(
        self,
        from_infos,
        to_infos,
        names=None,
        no_progress_bar=False,
        resume=False,
    ):
        names = self._verify_path_args(to_infos, from_infos, names)
        fails = 0

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != self.scheme:
                raise NotImplementedError

            if to_info.scheme != "local":
                raise NotImplementedError

            msg = "Downloading '{}' to '{}'".format(from_info, to_info)
            logger.debug(msg)

            if not name:
                name = to_info.name

            makedirs(fspath_py35(to_info.parent), exist_ok=True)

            total = self._content_length(from_info.url)

            if no_progress_bar or not total:
                cb = None
            else:
                cb = ProgressBarCallback(name, total)

            try:
                self._download_to(
                    from_info.url, to_info.fspath, callback=cb, resume=resume
                )

            except Exception:
                fails += 1
                msg = "failed to download '{}'".format(from_info)
                logger.exception(msg)
                continue

            if not no_progress_bar:
                progress.finish_target(name)

        return fails

    def exists(self, path_info):
        return bool(self._request("HEAD", path_info.url))

    def batch_exists(self, path_infos, callback):
        results = []

        for path_info in path_infos:
            results.append(self.exists(path_info))
            callback.update(str(path_info))

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
