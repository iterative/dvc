import os
import threading
import requests

from dvc.logger import Logger
from dvc.progress import progress
from dvc.exceptions import DvcException
from dvc.remote.base import RemoteBase


class Callback(object):
    def __init__(self, name, total):
        self.name = name
        self.total = total
        self.current = 0
        self.lock = threading.Lock()

    def __call__(self, byts):
        with self.lock:
            self.current += byts
            progress.update_target(self.name, self.current, self.total)


class RemoteHTTP(RemoteBase):
    REGEX = r'^https?://.*$'
    TIMEOUT_GET = 10
    CHUNK_SIZE = 128
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project

    def download(self,
                 from_infos,
                 to_infos,
                 no_progress_bar=False,
                 names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info['scheme'] not in ['http', 'https']:
                raise NotImplementedError

            if to_info['scheme'] != 'local':
                raise NotImplementedError

            msg = "Downloading '{}' to '{}'".format(from_info['url'],
                                                    to_info['path'])
            Logger.debug(msg)

            tmp_file = self.tmp_file(to_info['path'])
            if not name:
                name = os.path.basename(to_info['path'])

            self._makedirs(to_info['path'])

            try:
                if no_progress_bar:
                    cb = None
                else:
                    total = self._content_length(from_info['url'])
                    cb = Callback(name, total)

                self._download_to(from_info['url'], tmp_file)
            except Exception as exc:
                msg = "Failed to download '{}/{}'".format(from_info['bucket'],
                                                          from_info['key'])
                Logger.warn(msg, exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def exists(self, path_infos):
        return [
            bool(self._etag(path_info['url']))
            for path_info in path_infos
        ]

    def save_info(self, path_info):
        if path_info['scheme'] not in ['http', 'https']:
            raise NotImplementedError

        return {self.PARAM_ETAG: self._etag(path_info['url'])}

    def _content_length(self, url):
        r = requests.head(url, allow_redirects=True, timeout=self.TIMEOUT_GET)
        return r.headers.get('Content-Length')

    def _etag(self, url):
        r = requests.head(url, allow_redirects=True, timeout=self.TIMEOUT_GET)
        etag = r.headers.get('ETag')

        if not etag:
            raise DvcException('Could not find an ETag for that resource')

        if etag.startswith('W/'):
            raise DvcException('DVC do not support weak ETags for caching')

        return etag

    def _download_to(self, url, file, callback=None):
        r = requests.get(url,
                         allow_redirects=True,
                         stream=True,
                         timeout=self.TIMEOUT_GET)

        with open(file, 'wb') as fd:
            bytes_transfered = 0

            for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                fd.write(chunk)

                if callback:
                    bytes_transfered += len(chunk)
                    callback(bytes_transfered)
