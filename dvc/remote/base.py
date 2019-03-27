from __future__ import unicode_literals

from dvc.utils.compat import str

import re
import posixpath
from multiprocessing import cpu_count

import dvc.prompt as prompt
import dvc.logger as logger
from dvc.config import Config
from dvc.exceptions import DvcException, ConfirmRemoveError


STATUS_OK = 1
STATUS_MISSING = 2
STATUS_NEW = 3
STATUS_DELETED = 4


STATUS_MAP = {
    # (local_exists, remote_exists)
    (True, True): STATUS_OK,
    (False, False): STATUS_MISSING,
    (True, False): STATUS_NEW,
    (False, True): STATUS_DELETED,
}


class DataCloudError(DvcException):
    """ Data Cloud exception """

    def __init__(self, msg):
        super(DataCloudError, self).__init__("Data sync error: {}".format(msg))


class RemoteCmdError(DvcException):
    def __init__(self, remote, cmd, ret, err):
        super(RemoteCmdError, self).__init__(
            "{remote} command '{cmd}' finished with non-zero return code"
            " {ret}': {err}".format(remote=remote, cmd=cmd, ret=ret, err=err)
        )


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = "{} is not supported by {} remote".format(action, scheme)
        super(RemoteActionNotImplemented, self).__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


class RemoteBase(object):
    scheme = None
    REGEX = None
    REQUIRES = {}
    JOBS = 4 * cpu_count()

    def __init__(self, repo, config):
        self.repo = repo
        deps_ok = all(self.REQUIRES.values())
        if not deps_ok:
            missing = [k for k, v in self.REQUIRES.items() if v is None]
            url = config.get(
                Config.SECTION_REMOTE_URL, "{}://".format(self.scheme)
            )
            msg = (
                "URL '{}' is supported but requires these missing "
                "dependencies: {}. If you have installed dvc using pip, "
                "choose one of these options to proceed: \n"
                "\n"
                "    1) Install specific missing dependencies:\n"
                "        pip install {}\n"
                "    2) Install dvc package that includes those missing "
                "dependencies: \n"
                "        pip install dvc[{}]\n"
                "    3) Install dvc package with all possible "
                "dependencies included: \n"
                "        pip install dvc[all]\n"
                "\n"
                "If you have installed dvc from a binary package and you "
                "are still seeing this message, please report it to us "
                "using https://github.com/iterative/dvc/issues. Thank you!"
            ).format(url, missing, " ".join(missing), self.scheme)
            raise RemoteMissingDepsError(msg)

    def __repr__(self):
        return "{class_name}: '{url}'".format(
            class_name=type(self).__name__, url=(self.url or "No url")
        )

    def compat_config(config):
        return config.copy()

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_REMOTE_URL]
        return cls.match(url) is not None

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def group(self, name):
        m = self.match(self.url)
        if not m:
            return None
        return m.group(name)

    def save_info(self, path_info):
        raise NotImplementedError

    def changed(self, path_info, checksum_info):
        """Checks if data has changed.

        A file is considered changed if:
            - It doesn't exist on the working directory (was unlinked)
            - Checksum is not computed (saving a new file)
            - The checkusm stored in the State is different from the given one
            - There's no file in the cache

        Args:
            path_info: dict with path information.
            checksum: expected checksum for this data.

        Returns:
            bool: True if data has changed, False otherwise.
        """

        logger.debug(
            "checking if '{}'('{}') has changed.".format(
                path_info, checksum_info
            )
        )

        if not self.exists(path_info):
            logger.debug("'{}' doesn't exist.".format(path_info))
            return True

        checksum = checksum_info.get(self.PARAM_CHECKSUM)
        if checksum is None:
            logger.debug("checksum for '{}' is missing.".format(path_info))
            return True

        if self.changed_cache(checksum):
            logger.debug(
                "cache for '{}'('{}') has changed.".format(path_info, checksum)
            )
            return True

        actual = self.save_info(path_info)[self.PARAM_CHECKSUM]
        if checksum != actual:
            logger.debug(
                "checksum '{}'(actual '{}') for '{}' has changed.".format(
                    checksum, actual, path_info
                )
            )
            return True

        logger.debug("'{}' hasn't changed.".format(path_info))
        return False

    def save(self, path_info, checksum_info):
        if path_info["scheme"] != self.scheme:
            raise RemoteActionNotImplemented(
                "save {} -> {}".format(path_info["scheme"], self.scheme),
                self.scheme,
            )

        checksum = checksum_info[self.PARAM_CHECKSUM]
        if not self.changed_cache(checksum):
            return

        to_info = self.checksum_to_path_info(checksum)

        logger.info("Saving '{}' to '{}'.".format(path_info, to_info))

        self.copy(path_info, to_info)

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        name=None,
        resume=False,
    ):
        raise RemoteActionNotImplemented("download", self.scheme)

    def upload(self, from_infos, to_infos, names=None):
        raise RemoteActionNotImplemented("upload", self.scheme)

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def move(self, path_info):
        raise RemoteActionNotImplemented("move", self.scheme)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

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
        path_info["path"] = self.checksum_to_path(checksum)
        return path_info

    def md5s_to_path_infos(self, md5s):
        return [self.checksum_to_path_info(md5) for md5 in md5s]

    def list_cache_paths(self):
        raise NotImplementedError

    def all(self):
        # NOTE: The list might be way too big(e.g. 100M entries, md5 for each
        # is 32 bytes, so ~3200Mb list) and we don't really need all of it at
        # the same time, so it makes sense to use a generator to gradually
        # iterate over it, without keeping all of it in memory.
        return (
            self.path_to_checksum(path) for path in self.list_cache_paths()
        )

    def gc(self, cinfos):
        from dvc.remote.local import RemoteLOCAL

        used = [info[RemoteLOCAL.PARAM_CHECKSUM] for info in cinfos["local"]]

        if self.scheme != "":
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

        if not self.exists(cache):
            return True

        actual = self.save_info(cache)

        logger.debug(
            "Cache '{}' actual '{}'.".format(str(expected), str(actual))
        )

        if expected != actual:
            if self.exists(cache):
                msg = "corrupted cache file {}"
                logger.warning(msg.format(str(cache)))
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
                "file '{}' is going to be removed."
                " Are you sure you want to proceed?".format(str(path_info))
            )

            if not prompt.confirm(msg):
                raise ConfirmRemoveError(str(path_info))

        self.remove(path_info)

    def do_checkout(
        self, path_info, checksum, force=False, progress_callback=None
    ):
        if self.exists(path_info):
            msg = "data '{}' exists. Removing before checkout."
            logger.warning(msg.format(str(path_info)))
            self.safe_remove(path_info, force=force)

        from_info = self.checksum_to_path_info(checksum)
        self.copy(from_info, path_info)

    def checkout(
        self, path_info, checksum_info, force=False, progress_callback=None
    ):
        scheme = path_info["scheme"]
        if scheme not in ["", "local"] and scheme != self.scheme:
            raise NotImplementedError

        checksum = checksum_info.get(self.PARAM_CHECKSUM)
        if not checksum:
            msg = "No checksum info for '{}'."
            logger.debug(msg.format(str(path_info)))
            return

        if not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            logger.debug(msg.format(str(path_info)))
            return

        if self.changed_cache(checksum):
            msg = "Cache '{}' not found. File '{}' won't be created."
            logger.warning(msg.format(checksum, str(path_info)))
            self.safe_remove(path_info, force=force)
            return

        msg = "Checking out '{}' with cache '{}'."
        logger.debug(msg.format(str(path_info), checksum))

        self.do_checkout(
            path_info,
            checksum,
            force=force,
            progress_callback=progress_callback,
        )

    @staticmethod
    def unprotect(path_info):
        pass
