import os
import re
import errno
from multiprocessing import cpu_count

from dvc.config import Config
from dvc.logger import Logger
from dvc.exceptions import DvcException


STATUS_OK = 1
STATUS_NEW = 3
STATUS_DELETED = 4


STATUS_MAP = {
    # (local_exists, remote_exists)
    (True, True): STATUS_OK,
    (False, False): STATUS_OK,
    (True, False): STATUS_NEW,
    (False, True): STATUS_DELETED,
}


class DataCloudError(DvcException):
    """ Data Cloud exception """
    def __init__(self, msg):
        super(DataCloudError, self).__init__('Data sync error: {}'.format(msg))


class RemoteBase(object):
    REGEX = None
    REQUIRES = {}
    JOBS = 4 * cpu_count()

    def __init__(self, project, config):
        pass

    def compat_config(config):
        return config.copy()

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_REMOTE_URL]
        url_ok = cls.match(url) is not None
        deps_ok = all(cls.REQUIRES.values())
        if url_ok and not deps_ok:
            missing = [k for k, v in cls.REQUIRES.items() if v is None]
            msg = "URL \'{}\' is supported but requires these missing " \
                  "dependencies: {}. If you have installed dvc using pip, " \
                  "choose one of these options to proceed: \n" \
                  "\n" \
                  "    1) Install specific missing dependencies:\n" \
                  "        pip install {}\n" \
                  "    2) Install dvc package that includes those missing " \
                  "dependencies: \n" \
                  "        pip install dvc[{}]\n" \
                  "    3) Install dvc package with all possible " \
                  "dependencies included: \n" \
                  "        pip install dvc[all]\n" \
                  "\n" \
                  "If you have installed dvc from a binary package and you " \
                  "are still seeing this message, please report it to us " \
                  "using https://github.com/iterative/dvc/issues. Thank you!"
            msg = msg.format(url, missing, " ".join(missing), cls.scheme)
            Logger.warn(msg)

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
        # FIXME probably better use uuid()
        return fname + '.part'

    def save_info(self, path_info):
        raise NotImplementedError

    def changed(self, path_info, checksum_info):
        raise NotImplementedError

    def save(self, path_info):
        raise NotImplementedError

    def checkout(self, path_info, checksum_info):
        raise NotImplementedError

    def download(self, from_infos, to_infos, no_progress_bar=False, name=None):
        raise NotImplementedError

    def upload(self, from_infos, to_infos, path_info, name=None):
        raise NotImplementedError

    def remove(self, path_info):
        raise NotImplementedError

    def move(self, path_info):
        raise NotImplementedError

    def _makedirs(self, fname):
        dname = os.path.dirname(fname)

        try:
            os.makedirs(dname)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def md5s_to_path_infos(self, md5s):
        raise NotImplementedError

    def exists(self, path_infos):
        raise NotImplementedError

    @classmethod
    def _verify_path_args(cls, from_infos, to_infos, names=None):
        assert isinstance(from_infos, list)
        assert isinstance(to_infos, list)
        assert len(from_infos) == len(to_infos)

        if not names:
            names = len(to_infos) * [None]
        else:
            assert isinstance(names, list)
            assert len(names) == len(to_infos)

        return names
