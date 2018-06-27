import os
import re
import tempfile
import posixpath

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
    REQUIRES = {}

    def __init__(self, project, config):
        pass

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_REMOTE_URL]
        url_ok = cls.match(url)
        deps_ok = all(cls.REQUIRES.values())
        if url_ok and not deps_ok:
            missing = [k for k,v in cls.REQUIRES.items() if v == None]
            msg = "URL \'{}\' is supported but requires these missing dependencies: {}"
            Logger.warn(msg.format(url, str(missing)))
        return url_ok and deps_ok

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def group(self, name):
        m = self.match(self.url)
        if not m:
            return None
        return m.group(name)

    @staticmethod
    def tmp_file(fname):
        """ Temporary name for a partial download """
        #FIXME probably better use uuid()
        return fname + '.part'

    def save_info(self, path_info):
        raise NotImplementedError

    def save(self, path_info):
        raise NotImplementedError

    def checkout(self, path_info, checksum_info):
        raise NotImplementedError

    def _get_path_info(self, path):
        raise NotImplementedError

    def _new_path_info(self, path):
        raise NotImplementedError

    def download(self, path_info, path, no_progress_bar=False, name=None):
        raise NotImplementedError

    def upload(self, path, path_info, name=None):
        raise NotImplementedError

    def remove(self, path_info):
        raise NotImplementedError

    def move(self, path_info):
        raise NotImplementedError

    def cache_file_key(self, fname):
        """ Key of a file within the bucket """
        relpath = os.path.relpath(fname, self.project.cache.local.cache_dir)
        relpath = relpath.replace('\\', '/')
        return posixpath.join(self.prefix, relpath).strip('/')

    def collect(self, arg):
        from dvc.remote.local import RemoteLOCAL

        path, local = arg
        ret = [path]
        md5 = self.project.cache.local.path_to_md5(path)

        if not RemoteLOCAL.is_dir_cache(path):
            return ret

        if not local and self.project.cache.local.changed(md5):
            path_info = self._get_path_info(path)
            if not path_info:
                Logger.debug("File '{}' does not exist in the cloud".format(path))
                return ret
            self.download(path_info, path, no_progress_bar=True)

        if self.project.cache.local.changed(md5):
            return ret

        for relpath, md5 in RemoteLOCAL.get_dir_cache(path).items():
            cache = self.project.cache.local.get(md5)
            ret.append(cache)

        return ret

    def push(self, path):
        path_info = self._get_path_info(path)
        if path_info:
            Logger.debug("File '{}' already uploaded to the cloud.".format(path))
            return None

        path_info = self._new_path_info(path)
        md5 = self.project.cache.local.path_to_md5(path)

        if self.project.cache.local.changed(md5):
            return None

        self.upload(path, path_info, name=md5)

    def _makedirs(self, fname):
        dname = os.path.dirname(fname)
        try:
            os.makedirs(dname)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise

    def pull(self, path):
        """ Generic method for pulling data from the cloud """
        path_info = self._get_path_info(path)
        if not path_info:
            Logger.error("File '{}' does not exist in the cloud".format(path))
            return None

        md5 = self.project.cache.local.path_to_md5(path)

        self.download(path_info, path, name=md5)

        if self.project.cache.local.changed(md5):
            return None

    def status(self, path):
        """
        Generic method for checking data item status.
        """
        md5 = self.project.cache.local.path_to_md5(path)
        path_info = self._get_path_info(path)
        remote_exists = path_info != None
        local_exists = os.path.exists(path) and not self.project.cache.local.changed(md5)

        diff = None
        if remote_exists and local_exists:
            md5 = self.project.cache.local.path_to_md5(path)
            diff = not self.project.cache.local.changed(md5)

        return STATUS_MAP.get((local_exists, remote_exists, diff), STATUS_UNKNOWN)
