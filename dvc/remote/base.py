import os
import re
import errno
import posixpath
from multiprocessing import cpu_count

import dvc.prompt as prompt
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

    def download(self, from_infos, to_infos, no_progress_bar=False, name=None):
        raise NotImplementedError

    def upload(self, from_infos, to_infos, path_info, name=None):
        raise NotImplementedError

    def remove(self, path_info):
        raise NotImplementedError

    def move(self, path_info):
        raise NotImplementedError

    def copy(self, from_info, to_info):
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
        expected = {self.PARAM_CHECKSUM: checksum}
        actual = self.save_info(cache)

        logger.debug("Cache '{}' actual '{}'.".format(str(expected),
                                                      str(actual)))

        if expected != actual:
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
        # it in one pass.
        return list(filter(lambda checksum: checksum in checksums, self.all()))

    def already_cached(self, path_info):
        current = self.save_info(path_info)[self.PARAM_CHECKSUM]

        if not current:
            return False

        return not self.changed_cache(current)

    def safe_remove(self, path_info, force=False):
        if not self.exists(path_info):
            return

        if not force and not self.already_cached(path_info):
            msg = (
                'File "{}" is going to be removed. '
                'Are you sure you want to proceed?'
                .format(str(path_info))
            )

            if not prompt.confirm(msg):
                raise DvcException("Unable to remove {} without a confirmation"
                                   " from the user. Use '-f' to force."
                                   .format(str(path_info)))

        self.remove(path_info)

    def do_checkout(self, path_info, checksum, force=False):
        if self.exists(path_info):
            msg = "Data '{}' exists. Removing before checkout."
            logger.warn(msg.format(str(path_info)))
            self.safe_remove(path_info, force=force)

        from_info = self.checksum_to_path_info(checksum)
        self.copy(from_info, path_info)

    def checkout(self, path_info, checksum_info, force=False):
        scheme = path_info['scheme']
        if scheme not in ['', 'local'] and scheme != self.scheme:
            raise NotImplementedError

        checksum = checksum_info.get(self.PARAM_CHECKSUM)
        if not checksum:
            msg = "No checksum info for '{}'."
            logger.info(msg.format(str(path_info)))
            return

        if not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            logger.info(msg.format(str(path_info)))
            return

        if self.changed_cache(checksum):
            msg = "Cache '{}' not found. File '{}' won't be created."
            logger.warn(msg.format(checksum, str(path_info)))
            self.safe_remove(path_info, force=force)
            return

        msg = "Checking out '{}' with cache '{}'."
        logger.info(msg.format(str(path_info), checksum))

        self.do_checkout(path_info, checksum, force=force)
