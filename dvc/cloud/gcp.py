import base64
import os

from google.cloud import storage as gc

from dvc.logger import Logger
from dvc.config import ConfigError
from dvc.cloud.base import DataCloudError, DataCloudBase
from dvc.utils import file_md5
from dvc.progress import progress


class DataCloudGCP(DataCloudBase):
    """ DataCloud class for Google Cloud Platform """
    @property
    def gc_project_name(self):
        """
        Get project name from config.
        """
        return self._cloud_settings.cloud_config.get('ProjectName', None)

    def sanity_check(self):
        project = self.gc_project_name
        if project is None or len(project) < 1:
            raise ConfigError('can\'t read google cloud project name. '
                              'Please set ProjectName in section GC.')

    def connect(self):
        client = gc.Client(project=self.gc_project_name)
        self.bucket = client.bucket(self.storage_bucket)
        if not self.bucket.exists():
            raise DataCloudError('sync up: google cloud bucket {} '
                                 'doesn\'t exist'.format(self.storage_bucket))

    @staticmethod
    def _cmp_checksum(blob, fname):
        """
        Verify local and remote checksums.
        """
        md5 = file_md5(fname)[1]
        b64_encoded_md5 = base64.b64encode(md5).decode() if md5 else None

        if blob.md5_hash == b64_encoded_md5:
            return True

        return False

    def _pull_key(self, key, path, no_progress_bar=False):
        self._makedirs(path)

        name = os.path.relpath(path, self._cloud_settings.cache.cache_dir)
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
        return self.bucket.get_blob(key_name)

    def _new_key(self, path):
        key_name = self.cache_file_key(path)
        return self.bucket.blob(key_name)

    def _push_key(self, key, path):
        """ push, gcp version """
        name = os.path.relpath(path, self._cloud_settings.cache.cache_dir)

        progress.update_target(name, 0, None)

        key.upload_from_filename(path)

        progress.finish_target(name)
        Logger.debug('uploading %s completed' % path)

        return path
