import os

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError


STATUS_UNKNOWN = 0
STATUS_OK = 1
STATUS_MODIFIED = 2
STATUS_NEW = 3
STATUS_DELETED = 4


STATUS_MAP = {
    # (local_exists, remote_exists, cmp)
    (True, True, True)  : STATUS_OK,
    (True, True, False) : STATUS_MODIFIED,
    (True, False, None) : STATUS_NEW,
    (False, True, None) : STATUS_DELETED,
}


class DataCloudError(DvcException):
    """ Data Cloud exception """
    def __init__(self, msg):
        super(DataCloudError, self).__init__('Data sync error: {}'.format(msg))


class DataCloudBase(object):
    """ Base class for DataCloud """
    def __init__(self, cloud_settings):
        self._cloud_settings = cloud_settings

    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        if self._cloud_settings.global_storage_path:
            return self._cloud_settings.global_storage_path

        path = self._cloud_settings.cloud_config.get('StoragePath', None)
        if path is None:
            raise ConfigError('invalid StoragePath: not set for Data or cloud specific')

        return path

    def _storage_path_parts(self):
        """
        Split storage path into parts. I.e. 'dvc-test/myrepo' -> ['dvc', 'myrepo']
        """
        return self.storage_path.strip('/').split('/', 1)

    @property
    def storage_bucket(self):
        """ Data -> StoragePath takes precedence; if doesn't exist, use cloud-specific """
        return self._storage_path_parts()[0]

    @property
    def storage_prefix(self):
        """
        Prefix within the bucket. I.e. 'myrepo' in 'dvc-test/myrepo'.
        """
        parts = self._storage_path_parts()
        if len(parts) > 1:
            return parts[1]
        return ''

    def cache_file_key(self, fname):
        """ Key of a file within the bucket """
        return '{}/{}'.format(self.storage_prefix, os.path.basename(fname)).strip('/')

    @staticmethod
    def tmp_file(fname):
        """ Temporary name for a partial download """
        return fname + '.part'

    def sanity_check(self):
        """
        Cloud-specific method to check config for basic requirements.
        """
        pass

    def _import(self, bucket, fin, fout):
        """
        Cloud-specific method for importing data file.
        """
        pass

    def push(self, path):
        """ Cloud-specific method for pushing data """
        pass

    def pull(self, path):
        """ Generic method for pulling data from the cloud """
        key_name = self.cache_file_key(path)
        return self._import(self.storage_bucket, key_name, path)

    def remove(self, path):
        """
        Cloud-specific method for removing data item from the cloud.
        """
        pass

    def _status(self, path):
        """
        Cloud-specific method for checking data item status.
        """
        pass

    def status(self, path):
        """
        Generic method for checking data item status.
        """
        return STATUS_MAP.get(self._status(path), STATUS_UNKNOWN)
