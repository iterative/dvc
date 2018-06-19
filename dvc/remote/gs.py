import os
import posixpath
from google.cloud import storage

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.logger import Logger
from dvc.remote.base import RemoteBase
from dvc.config import Config
from dvc.progress import progress


class RemoteGS(RemoteBase):
    scheme = 'gs'
    REGEX = r'^gs://(?P<path>.*)$'
    PARAM_ETAG = 'etag'

    def __init__(self, project, config):
        self.project = project
        storagepath = 'gs://' + config.get(Config.SECTION_AWS_STORAGEPATH, '/').lstrip('/')
        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.projectname = config.get(Config.SECTION_GCP_PROJECTNAME, None)

    @property
    def bucket(self):
        return urlparse(self.url).netloc

    @property
    def prefix(self):
        return urlparse(self.url).path.lstrip('/')

    @property
    def gs(self):
        return storage.Client()

    def get_etag(self, bucket, key):
        blob = self.gs.bucket(bucket).get_blob(key)
        if not blob:
            return None

        return blob.etag

    def save_info(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        return {self.PARAM_ETAG: self.get_etag(path_info['bucket'], path_info['key'])}

    def save(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = self.get_etag(path_info['bucket'], path_info['key'])
        dest_key = posixpath.join(self.prefix, etag[0:2], etag[2:])

        blob = self.gs.bucket(path_info['bucket']).get_blob(path_info['key'])
        if not blob:
            raise DvcException('{} doesn\'t exist in the cloud'.format(path_info['key']))

        self.gs.bucket(self.bucket).copy_blob(blob, self.gs.bucket(path_info['bucket']), new_name=dest_key)

        return {self.PARAM_ETAG: etag}

    def checkout(self, path_info, checksum_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        etag = checksum_info.get(self.PARAM_ETAG, None)
        if not etag:
            return

        key = posixpath.join(self.prefix, etag[0:2], etag[2:])
        blob = self.gs.bucket(self.bucket).get_blob(key)
        if not blob:
            raise DvcException('{} doesn\'t exist in the cloud'.format(key))

        self.gs.bucket(path_info['bucket']).copy_blob(blob, self.gs.bucket(self.bucket), new_name=path_info['key'])

    def remove(self, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        blob = self.gs.bucket(path_info['bucket']).get_blob(path_info['key'])
        if not blob:
            return

        blob.delete()

    def upload(self, path, path_info):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        blob = self.gs.bucket(path_info['bucket']).blob(path_info['key'])
        blob.upload_from_filename(path)

    def download(self, path_info, path):
        if path_info['scheme'] != 'gs':
            raise NotImplementedError

        blob = self.gs.bucket(path_info['bucket']).get_blob(path_info['key'])
        blob.download_to_filename(path)

    # old code from here
    def _pull_key(self, key, path, no_progress_bar=False):
        self._makedirs(path)

        name = os.path.relpath(path, self.project.cache.local.cache_dir)
        tmp_file = self.tmp_file(path)

        if self._cmp_checksum(key, path):
            Logger.debug('File "{}" matches with "{}".'.format(path, key.name))
            return path

        Logger.debug('Downloading cache file from gc "{}/{}"'.format(key.bucket.name, key.name))

        if not no_progress_bar:
            # percent_cb is not available for download_to_filename, so
            # lets at least update progress at keypoints(start, finish)
            progress.update_target(name, 0, None)

        try:
            key.download_to_filename(tmp_file)
        except Exception as exc:
            Logger.error('Failed to download "{}": {}'.format(key.name, exc))
            return None

        os.rename(tmp_file, path)

        if not no_progress_bar:
            progress.finish_target(name)

        Logger.debug('Downloading completed')

        return path

    def _get_key(self, path):
        key_name = self.cache_file_key(path)
        return self.gs.bucket(self.bucket).get_blob(key_name)

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        return self.gs.bucket(self.bucket).blob(key_name)

    def _push_key(self, key, path):
        """ push, gcp version """
        name = os.path.relpath(path, self.project.cache.local.cache_dir)

        progress.update_target(name, 0, None)

        key.upload_from_filename(path)

        progress.finish_target(name)
        Logger.debug('uploading %s completed' % path)

        return path
