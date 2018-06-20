import os
import re
import tempfile

from dvc.config import Config
from dvc.logger import Logger
from dvc.exceptions import DvcException


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



class RemoteBase(object):
    REGEX = None

    def __init__(self, project, config):
        pass

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_REMOTE_URL]
        return cls.match(url) != None

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def save_info(self, path_info):
        raise NotImplementedError

    def save(self, path_info):
        raise NotImplementedError

    def checkout(self, path_info, checksum_info):
        raise NotImplementedError

    def download(self, path_info, path):
        raise NotImplementedError

    def upload(self, path, path_info):
        raise NotImplementedError

    # Old code starting from here

    def cache_file_key(self, fname):
        """ Key of a file within the bucket """
        relpath = os.path.relpath(fname, self.project.cache.local.cache_dir)
        relpath = relpath.replace('\\', '/')
        return '{}/{}'.format(self.prefix, relpath).strip('/')

    def cache_key_name(self, path):
        relpath = os.path.relpath(path, self.project.cache.local.cache_dir)
        return relpath.replace('\\', '').replace('/', '')

    @staticmethod
    def tmp_file(fname):
        """ Temporary name for a partial download """
        return fname + '.part'

    def _push_key(self, key, path):
        pass

    def collect(self, arg):
        from dvc.remote.local import RemoteLOCAL

        path, local = arg
        ret = [path]

        if not RemoteLOCAL.is_dir_cache(path):
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

        for relpath, md5 in RemoteLOCAL.get_dir_cache(dir_path).items():
            cache = self.project.cache.local.get(md5)
            ret.append(cache)

        return ret

    def _cmp_checksum(self, blob, fname):
        md5 = self.project.cache.local.path_to_md5(fname)
        if self.project.cache.local.state.changed(fname, md5=md5):
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
