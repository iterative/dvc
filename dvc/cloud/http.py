import os
import requests

from dvc.logger import Logger
from dvc.progress import progress
from dvc.utils import copyfile
from dvc.cloud.base import DataCloudError, DataCloudBase
from dvc.utils import file_md5


class DataCloudHTTP(DataCloudBase):
    """
    Driver for http cloud.
    """
    def push(self, path):
        raise Exception('Not implemented yet')

    def pull(self, path):
        raise Exception('Not implemented yet')

    def remove(self, path):
        raise Exception('Not implemented yet')

    def status(self, path):
        raise Exception('Not implemented yet')

    @staticmethod
    def _downloaded_size(fname):
        """
        Check how much is already downloaded.
        """
        if os.path.exists(fname):
            downloaded = os.path.getsize(fname)
            header = {'Range': 'bytes=%d-' % downloaded}

            Logger.debug('found existing {} file, resuming download'.format(fname))

            return (downloaded, header)

        return (0, None)

    @staticmethod
    def _get_header(req, name):
        """
        Get header value from request.
        """
        val = req.headers.get(name)
        if val == None:
            Logger.debug('\'{}\' not supported by the server'.format(name))

        return val

    def _verify_downloaded_size(self, req, downloaded_size):
        """
        Check that server supports resuming downloads.
        """
        content_range = self._get_header(req, 'content-range')
        if downloaded_size and content_range == None:
            Logger.debug('Can\'t resume download')
            return 0

        return downloaded_size

    def _download(self, req, fname, downloaded):
        """
        Download file with progress bar.
        """
        mode = 'ab' if downloaded else 'wb'
        name = os.path.basename(req.url)
        total_length = self._get_header(req, 'content-length')
        chunk_size = 1024 * 100

        progress.update_target(name, downloaded, total_length)

        with open(fname, mode) as fobj:
            for chunk in req.iter_content(chunk_size=chunk_size):
                if not chunk:  # filter out keep-alive new chunks
                    continue

                fobj.write(chunk)
                downloaded += len(chunk)
                progress.update_target(name, downloaded, total_length)


        progress.finish_target(name)

    def _verify_md5(self, req, fname):
        """
        Verify md5 of a downloaded file if server supports 'content-md5' header.
        """
        md5 = file_md5(fname)[0]
        content_md5 = self._get_header(req, 'content-md5')

        if content_md5 == None:
            return True

        if md5 != content_md5:
            Logger.error('Checksum mismatch')
            return False

        Logger.debug('Checksum matches')
        return True

    def import_data(self, url, path):
        """
        Download single file from url.
        """

        tmp_file = self.tmp_file(path)

        downloaded, header = self._downloaded_size(tmp_file)
        req = requests.get(url, stream=True, headers=header)
        downloaded = self._verify_downloaded_size(req, downloaded)

        try:
            self._download(req, tmp_file, downloaded)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(url, exc))
            return None

        if not self._verify_md5(req, tmp_file):
            return None

        os.rename(tmp_file, path)

        return path
