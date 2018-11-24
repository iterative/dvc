import os
import threading
import requests
import posixpath

from dvc.logger import Logger
from dvc.progress import progress
from dvc.exceptions import DvcException
from dvc.config import Config
from dvc.remote.base import RemoteBase


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


class RemoteHTTP(RemoteBase):
    scheme = 'http'
    REGEX = r'^https?://.*$'
    REQUEST_TIMEOUT = 10
    CHUNK_SIZE = 1000000  # Megabyte
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project
        self.cache_dir = config.get(Config.SECTION_REMOTE_URL)

    @property
    def prefix(self):
        return self.cache_dir

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

            total = self._content_length(from_info['url'])

            if no_progress_bar or not total:
                cb = None
            else:
                cb = ProgressBarCallback(name, total)

            try:
                self._download_to(from_info['url'], tmp_file, callback=cb)
            except Exception as exc:
                msg = "Failed to download '{}'".format(from_info['url'])
                Logger.warn(msg, exc)
                continue

            os.rename(tmp_file, to_info['path'])

            if not no_progress_bar:
                progress.finish_target(name)

    def exists(self, path_infos):
        return [
            bool(self._request('HEAD', path_info.get('url')))
            for path_info in path_infos
        ]

    def save_info(self, path_info):
        if path_info['scheme'] not in ['http', 'https']:
            raise NotImplementedError

        return {self.PARAM_ETAG: self._etag(path_info['url'])}

    def md5s_to_path_infos(self, md5s):
        return [
            {
                'scheme': 'http',
                'url': posixpath.join(self.prefix, md5[0:2], md5[2:]),
            }
            for md5 in md5s
        ]

    def _content_length(self, url):
        return self._request('HEAD', url).headers.get('Content-Length')

    def _etag(self, url):
        etag = self._request('HEAD', url).headers.get('ETag')

        if not etag:
            raise DvcException("Could not find an ETag for '{}'".format(url))

        if etag.startswith('W/'):
            raise DvcException(
                "Weak ETags are not supported."
                " (Etag: '{etag}', URL: '{url}')".format(etag=etag, url=url)
            )

        return etag

    def _download_to(self, url, file, callback=None):
        r = self._request('GET', url, stream=True)

        with open(file, 'wb') as fd:
            bytes_transfered = 0

            for chunk in r.iter_content(chunk_size=self.CHUNK_SIZE):
                fd.write(chunk)

                if callback:
                    bytes_transfered += len(chunk)
                    callback(bytes_transfered)

    def _request(self, method, url, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        kwargs.setdefault('timeout', self.REQUEST_TIMEOUT)

        try:
            return requests.request(method, url, **kwargs)
        except requests.exceptions.RequestException:
            raise DvcException('Could not perform a {} request'.format(method))
