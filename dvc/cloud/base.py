import os
import re
import tempfile

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import Config, ConfigError
from dvc.cache import Cache


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


class CloudSettings(object):
    def __init__(self, cache=None, global_storage_path=None, cloud_config=None):
        self.cache = cache
        self.cloud_config = cloud_config
        self.global_storage_path = global_storage_path


class DataCloudBase(object):
    """ Base class for DataCloud """

    REGEX = r''

    def __init__(self, cloud_settings):
        self._cloud_settings = cloud_settings

    @property
    def url(self):
        config = self._cloud_settings.cloud_config
        return config.get(Config.SECTION_REMOTE_URL, None)

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def group(self, group):
        return self.match(self.url).group(group)

    @property
    def path(self):
        return self.group('path')

    @classmethod
    def supported(cls, url):
        return cls.match(url) != None

    # backward compatibility
    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        if self._cloud_settings.global_storage_path:
            return self._cloud_settings.global_storage_path

        if not self.url:
            path = self._cloud_settings.cloud_config.get(Config.SECTION_CORE_STORAGEPATH, None)
            if path:
                Logger.warn('Using obsoleted config format. Consider updating.')
        else:
            path = self.path

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
        relpath = relpath.replace('\\', '/')
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

    def collect(self, arg):
        path, local = arg
        ret = [path]

        if not Cache.is_dir_cache(path):
            return ret

        if local:
            if not os.path.isfile(path):
                return ret
            dir_path = path
        else:
            key = self._get_key(path)
            if not key:
                Logger.debug("File '{}' does not exist in the cloud".format(path))
                return ret
            tmp = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
            self._pull_key(key, tmp, no_progress_bar=True)
            dir_path = tmp

        for relpath, md5 in Cache.get_dir_cache(dir_path).items():
            cache = self._cloud_settings.cache.get(md5)
            ret.append(cache)

        return ret

    def _cmp_checksum(self, blob, fname):
        md5 = self._cloud_settings.cache.path_to_md5(fname)
        if self._cloud_settings.cache.state.changed(fname, md5=md5):
            return False

        return True

    def push(self, path):
        key = self._get_key(path)
        if key:
            Logger.debug("File '{}' already uploaded to the cloud. Validating checksum...".format(path))
            if self._cmp_checksum(key, path):
                Logger.debug('File checksum matches. No uploading is needed.')
                return []
            Logger.debug('Checksum mismatch. Reuploading is required.')

        key = self._new_key(path)
        return self._push_key(key, path)

    def _makedirs(self, fname):
        dname = os.path.dirname(fname)
        try:
            os.makedirs(dname)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise

    def _pull_key(self, key, path, no_progress_bar=False):
        """ Cloud-specific method of pulling keys """
        pass

    def _get_key(self, path):
        """ Cloud-specific method of getting keys """
        pass

    def pull(self, path):
        """ Generic method for pulling data from the cloud """
        key = self._get_key(path)
        if not key:
            Logger.error("File '{}' does not exist in the cloud".format(path))
            return None

        return self._pull_key(key, path)

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
        if not key:
            return STATUS_NEW

        return self._status(key, path)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, tb):
        self.disconnect()
