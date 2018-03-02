import os
import yaml
import tempfile
from checksumdir import dirhash

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.stage import Output


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
        relpath = os.path.relpath(fname, self._cloud_settings.cache.cache_dir)
        relpath.replace('\\', '/')
        return '{}/{}'.format(self.storage_prefix, relpath).strip('/')

    @staticmethod
    def tmp_file(fname):
        """ Temporary name for a partial download """
        return fname + '.part'

    def sanity_check(self):
        """
        Cloud-specific method to check config for basic requirements.
        """
        pass

    def _push_key(self, key, path):
        pass

    def push(self, path):
        """ Generic method for pushing data """
        if os.path.isfile(path):
            return self._push(path)

        res = []
        for root, dirs, files in os.walk(path):
            for fname in files:
                p = os.path.join(root, fname)
                res.append(self._push(p))

                with open(p, 'r') as fd:
                    d = yaml.safe_load(fd)
                md5 = d[Output.PARAM_MD5]
                cache = self._cloud_settings.cache.get(md5)
                res.append(self._push(cache))
        return res

    def _push(self, path):
        key = self._get_key(path)
        if key:
            Logger.debug("File '{}' already uploaded to the cloud. Validating checksum...".format(path))
            if self._cmp_checksum(key, path):
                Logger.debug('File checksum matches. No uploading is needed.')
                return []
            Logger.debug('Checksum mismatch. Reuploading is required.')

        key = self._new_key(path)
        return [self._push_key(key, path)]

    def _makedirs(self, fname):
        dname = os.path.dirname(fname)
        if not os.path.exists(dname):
            os.makedirs(dname)

    def _pull_key(self, key, path):
        """ Cloud-specific method of pulling keys """
        pass

    def _get_key(self, path):
        """ Cloud-specific method of getting keys """
        pass

    def _get_keys(self, path):
        """ Cloud-specific method of getting keys """
        pass

    def pull(self, path):
        """ Generic method for pulling data from the cloud """
        key = self._get_key(path)
        if key:
            return [self._pull_key(key, path)]

        keys = self._get_keys(path)
        if not keys:
            Logger.error("File '{}' does not exist in the cloud".format(path))
            return []

        res = []
        for k in keys:
            fname = os.path.join(path, os.path.relpath(k.name, self.cache_file_key(path)))
            res.append(self._pull_key(k, fname))
            with open(fname, 'r') as fd:
                d = yaml.safe_load(fd)
            md5 = d[Output.PARAM_MD5]

            cache = self._cloud_settings.cache.get(md5)
            cache_key = self._get_key(cache)
            if not cache_key:
                Logger.error("File '{}' does not exist in the cloud".format(path))
                continue

            res.append(self._pull_key(cache_key, cache))

        return res

    def _status(self, key, path):
        remote_exists = key != None
        local_exists = os.path.exists(path)

        diff = None
        if remote_exists and local_exists:
            diff = self._cmp_checksum(key, path)

        return STATUS_MAP.get((local_exists, remote_exists, diff), STATUS_UNKNOWN)

    def status(self, path):
        """
        Generic method for checking data item status.
        """
        key = self._get_key(path)
        if key:
            return self._status(key, path)

        keys = self._get_keys(path)
        if not keys or len(keys) == 0:
            return STATUS_NEW

        res = []
        for k in keys:
            fname = os.path.join(path, os.path.relpath(k.name, self.cache_file_key(path)))
            res.append(self._status(k, fname))

            tmp = os.path.join(tempfile.mkdtemp(), k.name)
            self._pull_key(k, tmp)
            with open(tmp, 'r') as fd:
                d = yaml.safe_load(fd)
            md5 = d[Output.PARAM_MD5]

            cache = self._cloud_settings.cache.get(md5)
            cache_key = self._get_key(cache)
            res.append(self._status(cache_key, cache))

        return max(res)
