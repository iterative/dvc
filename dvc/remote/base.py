import os
import re
import errno
import posixpath
from multiprocessing import cpu_count

from dvc.config import Config
from dvc.logger import logger
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


class RemoteBaseCmdError(DvcException):
    def __init__(self, cmd, ret, err):
        m = "SSH command '{}' finished with non-zero return code '{}': {}"
        super(RemoteBaseCmdError, self).__init__(m.format(cmd, ret, err))


class RemoteBase(object):
    REGEX = None
    REQUIRES = {}
    JOBS = 4 * cpu_count()

    def __init__(self, project, config):
        pass

    def __repr__(self):
        return "{class_name}: '{url}'".format(
            class_name=type(self).__name__,
            url=(self.url or 'No url')
        )

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
            logger.warn(msg)

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
        import uuid
        return fname + '.' + str(uuid.uuid4())

    def save_info(self, path_info):
        raise NotImplementedError

    def changed(self, path_info, checksum_info):
        raise NotImplementedError

    def save(self, path_info):
        raise NotImplementedError

    def checkout(self, path_info, checksum_info, force=False):
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

    @property
    def ospath(self):
        return posixpath

    def checksum_to_path(self, checksum):
        return self.ospath.join(self.prefix, checksum[0:2], checksum[2:])

    def path_to_checksum(self, path):
        relpath = self.ospath.relpath(path, self.prefix)
        return self.ospath.dirname(relpath) + self.ospath.basename(relpath)

    def checksum_to_path_info(self, checksum):
        path_info = self.path_info.copy()
        path_info['path'] = self.checksum_to_path(checksum)
        return path_info

    def md5s_to_path_infos(self, md5s):
        return [
            self.checksum_to_path_info(md5)
            for md5 in md5s
        ]

    def list_cache_paths(self):
        raise NotImplementedError

    def all(self):
        # NOTE: The list might be way too big(e.g. 100M entries, md5 for each
        # is 32 bytes, so ~3200Mb list) and we don't really need all of it at
        # the same time, so it makes sense to use a generator to gradually
        # iterate over it, without keeping all of it in memory.
        return (
            self.path_to_checksum(path)
            for path in self.list_cache_paths()
        )

    def gc(self, cinfos):
        from dvc.remote.local import RemoteLOCAL

        used = [info[RemoteLOCAL.PARAM_CHECKSUM] for info in cinfos['local']]

        if self.scheme != '':
            used += [info[self.PARAM_CHECKSUM] for info in cinfos[self.scheme]]

        removed = False
        for checksum in self.all():
            if checksum in used:
                continue
            path_info = self.checksum_to_path_info(checksum)
            self.remove(path_info)
            removed = True
        return removed

    def changed_cache(self, checksum):
        cache = self.checksum_to_path_info(checksum)

        if {self.PARAM_CHECKSUM: checksum} != self.save_info(cache):
            if self.exists(cache):
                msg = 'Corrupted cache file {}'
                logger.warn(msg.format(str(cache)))
                self.remove(cache)
            return True

        return False

    def cache_exists(self, checksums):
        # NOTE: The reason for such an odd logic is that most of the remotes
        # take much shorter time to just retrieve everything they have under
        # a certain prefix(e.g. s3, gs, ssh, hdfs). Other remotes that can
        # check if particular file exists much quicker, use their own
        # implementation of cache_exists(see http, local).
        #
        # Result of all() might be way too big, so we should walk through
        # it in one pass. Also currently, cache_exists() should return
        # a list of True/False that matches order in checksums list,
        # so we need to use such an ugly logic.
        ret = len(checksums) * [False]
        for existing in self.all():
            for i, checksum in enumerate(checksums):
                if checksum == existing:
                    ret[i] = True
        return ret
