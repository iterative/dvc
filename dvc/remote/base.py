from __future__ import unicode_literals

from copy import copy

from dvc.path.local import PathLOCAL
from dvc.utils.compat import str

import os
import re
import json
import ntpath
import logging
import tempfile
import posixpath
from operator import itemgetter
from multiprocessing import cpu_count

import dvc.prompt as prompt
from dvc.config import Config
from dvc.exceptions import DvcException, ConfirmRemoveError
from dvc.progress import progress
from dvc.utils import LARGE_DIR_SIZE, tmp_fname
from dvc.state import StateBase


logger = logging.getLogger(__name__)


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


class RemoteBASE(object):
    scheme = None
    REGEX = None
    REQUIRES = {}
    JOBS = 4 * cpu_count()

    PARAM_RELPATH = "relpath"
    CHECKSUM_DIR_SUFFIX = ".dir"

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

        self.protected = False
        self.state = StateBase()

        self._dir_info = {}

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

    @staticmethod
    def _replace_sep(path, from_sep, to_sep):
        assert not ntpath.isabs(path)
        assert not posixpath.isabs(path)
        return path.replace(from_sep, to_sep)

    @classmethod
    def to_posixpath(cls, path):
        return cls._replace_sep(path, "\\", "/")

    @classmethod
    def to_ntpath(cls, path):
        return cls._replace_sep(path, "/", "\\")

    def to_ospath(self, path):
        if self.ospath == ntpath:
            return self.to_ntpath(path)
        elif self.ospath == posixpath:
            return self.to_posixpath(path)
        assert False

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def group(self, name):
        m = self.match(self.url)
        if not m:
            return None
        return m.group(name)

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    def get_file_checksum(self, path_info):
        raise NotImplementedError

    def _collect_dir(self, path_info):
        dir_info = []

        p_info = copy(path_info)
        dpath = p_info.path
        for root, dirs, files in self.walk(path_info):
            if len(files) > LARGE_DIR_SIZE:
                msg = (
                    "Computing md5 for a large directory {}. "
                    "This is only done once."
                )
                relpath = self.ospath.relpath(root)
                logger.info(msg.format(relpath))
                files = progress(files, name=relpath)

            for fname in files:
                path = self.ospath.join(root, fname)
                p_info.path = path
                relpath = self.to_posixpath(self.ospath.relpath(path, dpath))

                checksum = self.get_file_checksum(p_info)
                dir_info.append(
                    {
                        self.PARAM_RELPATH: relpath,
                        self.PARAM_CHECKSUM: checksum,
                    }
                )

        # NOTE: sorting the list by path to ensure reproducibility
        return sorted(dir_info, key=itemgetter(self.PARAM_RELPATH))

    def get_dir_checksum(self, path_info):
        dir_info = self._collect_dir(path_info)
        checksum, from_info, to_info = self._get_dir_info_checksum(
            dir_info, path_info
        )
        if self.cache.changed_cache_file(checksum):
            self.cache.move(from_info, to_info)

        self.state.save(path_info, checksum)
        self.state.save(to_info, checksum)

        return checksum

    def _get_dir_info_checksum(self, dir_info, path_info):
        to_info = copy(path_info)
        to_info.path = self.cache.ospath.join(self.cache.prefix, tmp_fname(""))

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        with open(tmp, "w+") as fobj:
            json.dump(dir_info, fobj, sort_keys=True)

        from_info = PathLOCAL(path=tmp)
        self.cache.upload([from_info], [to_info], no_progress_bar=True)

        checksum = self.get_file_checksum(to_info) + self.CHECKSUM_DIR_SUFFIX
        from_info = copy(to_info)
        to_info.path = self.cache.checksum_to_path(checksum)
        return checksum, from_info, to_info

    def get_dir_cache(self, checksum):
        assert checksum

        dir_info = self._dir_info.get(checksum)
        if dir_info:
            return dir_info

        dir_info = self.load_dir_cache(checksum)
        self._dir_info[checksum] = dir_info
        return dir_info

    def load_dir_cache(self, checksum):
        path_info = self.checksum_to_path_info(checksum)

        fobj = tempfile.NamedTemporaryFile(delete=False)
        path = fobj.name
        to_info = PathLOCAL(path=path)
        self.cache.download([path_info], [to_info], no_progress_bar=True)

        try:
            with open(path, "r") as fobj:
                d = json.load(fobj)
        except ValueError:
            logger.exception("Failed to load dir cache '{}'".format(path_info))
            return []
        finally:
            os.unlink(path)

        if not isinstance(d, list):
            msg = "dir cache file format error '{}' [skipping the file]"
            logger.error(msg.format(os.path.relpath(path)))
            return []

        for info in d:
            info[self.PARAM_RELPATH] = self.to_ospath(info[self.PARAM_RELPATH])

        return d

    @classmethod
    def is_dir_checksum(cls, checksum):
        return checksum.endswith(cls.CHECKSUM_DIR_SUFFIX)

    def get_checksum(self, path_info):
        if not self.exists(path_info):
            return None

        checksum = self.state.get(path_info)
        if checksum:
            return checksum

        if self.isdir(path_info):
            checksum = self.get_dir_checksum(path_info)
        else:
            checksum = self.get_file_checksum(path_info)

        if checksum:
            self.state.save(path_info, checksum)

        return checksum

    def save_info(self, path_info):
        assert path_info.scheme == self.scheme
        return {self.PARAM_CHECKSUM: self.get_checksum(path_info)}

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

    def link(self, from_info, to_info):
        self.copy(from_info, to_info)

    def _save_file(self, path_info, checksum, save_link=True):
        assert checksum

        cache_info = self.checksum_to_path_info(checksum)
        if self.changed_cache(checksum):
            self.move(path_info, cache_info)
        else:
            self.remove(path_info)

        self.link(cache_info, path_info)

        if save_link:
            self.state.save_link(path_info)

        # we need to update path and cache, since in case of reflink,
        # or copy cache type moving original file results in updates on
        # next executed command, which causes md5 recalculation
        self.state.save(path_info, checksum)
        self.state.save(cache_info, checksum)

    def _save_dir(self, path_info, checksum):
        cache_info = self.checksum_to_path_info(checksum)
        dir_info = self.get_dir_cache(checksum)

        entry_info = copy(path_info)
        for entry in dir_info:
            entry_checksum = entry[self.PARAM_CHECKSUM]
            entry_info.path = self.ospath.join(
                path_info.path, entry[self.PARAM_RELPATH]
            )

            self._save_file(entry_info, entry_checksum, save_link=False)

        self.state.save_link(path_info)
        self.state.save(cache_info, checksum)
        self.state.save(path_info, checksum)

    def is_empty(self, path_info):
        return False

    def isfile(self, path_info):
        raise NotImplementedError

    def isdir(self, path_info):
        return False

    def walk(self, path_info):
        raise NotImplementedError

    def protect(self, path_info):
        pass

    def save(self, path_info, checksum_info):
        if path_info.scheme != self.scheme:
            raise RemoteActionNotImplemented(
                "save {} -> {}".format(path_info.scheme, self.scheme),
                self.scheme,
            )

        checksum = checksum_info[self.PARAM_CHECKSUM]
        if not self.changed_cache(checksum):
            self._checkout(path_info, checksum)
            return

        self._save(path_info, checksum)

    def _save(self, path_info, checksum):
        to_info = self.checksum_to_path_info(checksum)
        logger.info("Saving '{}' to '{}'.".format(path_info, to_info))
        if self.isdir(path_info):
            self._save_dir(path_info, checksum)
            return
        self._save_file(path_info, checksum)

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        name=None,
        resume=False,
    ):
        raise RemoteActionNotImplemented("download", self.scheme)

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        raise RemoteActionNotImplemented("upload", self.scheme)

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def move(self, from_info, to_info):
        self.copy(from_info, to_info)
        self.remove(from_info)

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
        assert checksum
        return self.ospath.join(self.prefix, checksum[0:2], checksum[2:])

    def path_to_checksum(self, path):
        relpath = self.ospath.relpath(path, self.prefix)
        return self.ospath.dirname(relpath) + self.ospath.basename(relpath)

    def checksum_to_path_info(self, checksum):
        path_info = copy(self.path_info)
        path_info.path = self.checksum_to_path(checksum)
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

        used = {info[RemoteLOCAL.PARAM_CHECKSUM] for info in cinfos["local"]}

        if self.scheme != "":
            used |= {
                info[self.PARAM_CHECKSUM]
                for info in cinfos.get(self.scheme, [])
            }

        removed = False
        for checksum in self.all():
            if checksum in used:
                continue
            path_info = self.checksum_to_path_info(checksum)
            self.remove(path_info)
            removed = True
        return removed

    def changed_cache_file(self, checksum):
        cache_info = self.checksum_to_path_info(checksum)
        actual = self.get_checksum(cache_info)

        logger.debug(
            "cache '{}' expected '{}' actual '{}'".format(
                str(cache_info), checksum, actual
            )
        )

        if not checksum or not actual:
            return True

        if actual.split(".")[0] == checksum.split(".")[0]:
            return False

        if self.exists(cache_info):
            logger.warning("corrupted cache file '{}'.".format(cache_info))
            self.remove(cache_info)

        return True

    def _changed_cache_dir(self):
        # NOTE: only implemented for RemoteLOCAL
        return True

    def _changed_dir_cache(self, checksum):
        if not self._changed_cache_dir():
            return False

        if self.changed_cache_file(checksum):
            return True

        for entry in self.get_dir_cache(checksum):
            checksum = entry[self.PARAM_CHECKSUM]
            if self.changed_cache_file(checksum):
                return True

        return False

    def changed_cache(self, checksum):
        if self.is_dir_checksum(checksum):
            return self._changed_dir_cache(checksum)
        return self.changed_cache_file(checksum)

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
        current = self.get_checksum(path_info)

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

    def _checkout_file(
        self, path_info, checksum, force, progress_callback=None
    ):
        cache_info = self.checksum_to_path_info(checksum)
        if self.exists(path_info):
            msg = "data '{}' exists. Removing before checkout."
            logger.warning(msg.format(str(path_info)))
            self.safe_remove(path_info, force=force)

        self.link(cache_info, path_info)
        self.state.save_link(path_info)
        self.state.save(path_info, checksum)
        if progress_callback:
            progress_callback.update(path_info.url)

    def makedirs(self, path_info):
        raise NotImplementedError

    def _checkout_dir(
        self, path_info, checksum, force, progress_callback=None
    ):
        # Create dir separately so that dir is created
        # even if there are no files in it
        if not self.exists(path_info):
            self.makedirs(path_info)

        dir_info = self.get_dir_cache(checksum)

        logger.debug("Linking directory '{}'.".format(path_info))

        entry_info = copy(path_info)
        for entry in dir_info:
            relpath = entry[self.PARAM_RELPATH]
            entry_checksum = entry[self.PARAM_CHECKSUM]
            entry_cache_info = self.checksum_to_path_info(entry_checksum)
            entry_info.url = self.ospath.join(path_info.url, relpath)
            entry_info.path = self.ospath.join(path_info.path, relpath)

            entry_checksum_info = {self.PARAM_CHECKSUM: entry_checksum}
            if self.changed(entry_info, entry_checksum_info):
                if self.exists(entry_info):
                    self.safe_remove(entry_info, force=force)
                self.link(entry_cache_info, entry_info)
                self.state.save(entry_info, entry_checksum)
            if progress_callback:
                progress_callback.update(entry_info.url)

        self._remove_redundant_files(path_info, dir_info, force)

        self.state.save_link(path_info)
        self.state.save(path_info, checksum)

    def _remove_redundant_files(self, path_info, dir_info, force):
        existing_files = set(
            self.ospath.join(root, fname)
            for root, _, files in self.walk(path_info)
            for fname in files
        )

        needed_files = set(
            self.ospath.join(path_info.path, entry[self.PARAM_RELPATH])
            for entry in dir_info
        )

        delta = existing_files - needed_files

        d_info = copy(path_info)
        for path in delta:
            d_info.path = path
            self.safe_remove(d_info, force)

    def checkout(
        self, path_info, checksum_info, force=False, progress_callback=None
    ):
        if (
            path_info.scheme not in ["local"]
            and path_info.scheme != self.scheme
        ):
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

        return self._checkout(path_info, checksum, force, progress_callback)

    def _checkout(
        self, path_info, checksum, force=False, progress_callback=None
    ):
        if not self.is_dir_checksum(checksum):
            return self._checkout_file(
                path_info, checksum, force, progress_callback=progress_callback
            )
        return self._checkout_dir(
            path_info, checksum, force, progress_callback=progress_callback
        )

    @staticmethod
    def unprotect(path_info):
        pass
