from __future__ import unicode_literals

import errno

from dvc.utils.compat import basestring, FileNotFoundError, str, urlparse

import itertools
import json
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from functools import partial
from multiprocessing import cpu_count
from operator import itemgetter

from shortuuid import uuid

import dvc.prompt as prompt
from dvc.config import Config
from dvc.exceptions import (
    DvcException,
    ConfirmRemoveError,
    DvcIgnoreInCollectedDirError,
)
from dvc.ignore import DvcIgnore
from dvc.path_info import PathInfo, URLInfo
from dvc.progress import Tqdm
from dvc.remote.slow_link_detection import slow_link_guard
from dvc.state import StateNoop
from dvc.utils import makedirs, relpath, tmp_fname
from dvc.utils.fs import move
from dvc.utils.http import open_url

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


class RemoteCmdError(DvcException):
    def __init__(self, remote, cmd, ret, err):
        super(RemoteCmdError, self).__init__(
            "{remote} command '{cmd}' finished with non-zero return code"
            " {ret}': {err}".format(remote=remote, cmd=cmd, ret=ret, err=err)
        )


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = "{} is not supported for {} remotes".format(action, scheme)
        super(RemoteActionNotImplemented, self).__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


class DirCacheError(DvcException):
    def __init__(self, checksum, cause=None):
        super(DirCacheError, self).__init__(
            "Failed to load dir cache for checksum: '{}'.".format(checksum),
            cause=cause,
        )


class RemoteBASE(object):
    scheme = "base"
    path_cls = URLInfo
    REQUIRES = {}
    JOBS = 4 * cpu_count()

    PARAM_RELPATH = "relpath"
    CHECKSUM_DIR_SUFFIX = ".dir"
    CHECKSUM_JOBS = max(1, min(4, cpu_count() // 2))
    DEFAULT_CACHE_TYPES = ["copy"]

    state = StateNoop()

    def __init__(self, repo, config):
        self.repo = repo

        self._check_requires(config)

        core = config.get(Config.SECTION_CORE, {})
        self.checksum_jobs = core.get(
            Config.SECTION_CORE_CHECKSUM_JOBS, self.CHECKSUM_JOBS
        )

        self.protected = False
        self.no_traverse = config.get(Config.SECTION_REMOTE_NO_TRAVERSE, True)
        self._dir_info = {}

        types = config.get(Config.SECTION_CACHE_TYPE, None)
        if types:
            if isinstance(types, str):
                types = [t.strip() for t in types.split(",")]
            self.cache_types = types
        else:
            self.cache_types = copy(self.DEFAULT_CACHE_TYPES)
        self.cache_type_confirmed = False

    def _check_requires(self, config):
        import importlib

        missing = []

        for package, module in self.REQUIRES.items():
            try:
                importlib.import_module(module)
            except ImportError:
                missing.append(package)

        if not missing:
            return

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
            "        pip install 'dvc[{}]'\n"
            "    3) Install dvc package with all possible "
            "dependencies included: \n"
            "        pip install 'dvc[all]'\n"
            "\n"
            "If you have installed dvc from a binary package and you "
            "are still seeing this message, please report it to us "
            "using https://github.com/iterative/dvc/issues. Thank you!"
        ).format(url, missing, " ".join(missing), self.scheme)
        raise RemoteMissingDepsError(msg)

    def __repr__(self):
        return "{class_name}: '{path_info}'".format(
            class_name=type(self).__name__,
            path_info=self.path_info or "No path",
        )

    @classmethod
    def supported(cls, config):
        if isinstance(config, basestring):
            url = config
        else:
            url = config[Config.SECTION_REMOTE_URL]

        # NOTE: silently skipping remote, calling code should handle that
        parsed = urlparse(url)
        return parsed.scheme == cls.scheme

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    def get_file_checksum(self, path_info):
        raise NotImplementedError

    def _calculate_checksums(self, file_infos):
        file_infos = list(file_infos)
        with ThreadPoolExecutor(max_workers=self.checksum_jobs) as executor:
            tasks = executor.map(self.get_file_checksum, file_infos)
            with Tqdm(
                tasks,
                total=len(file_infos),
                unit="md5",
                desc="Computing hashes (only done once)",
            ) as tasks:
                checksums = dict(zip(file_infos, tasks))
        return checksums

    def _collect_dir(self, path_info):
        file_infos = set()

        for fname in self.walk_files(path_info):
            if DvcIgnore.DVCIGNORE_FILE == fname.name:
                raise DvcIgnoreInCollectedDirError(fname.parent)

            file_infos.add(fname)

        checksums = {fi: self.state.get(fi) for fi in file_infos}
        not_in_state = {
            fi for fi, checksum in checksums.items() if checksum is None
        }

        new_checksums = self._calculate_checksums(not_in_state)

        checksums.update(new_checksums)

        result = [
            {
                self.PARAM_CHECKSUM: checksums[fi],
                # NOTE: this is lossy transformation:
                #   "hey\there" -> "hey/there"
                #   "hey/there" -> "hey/there"
                # The latter is fine filename on Windows, which
                # will transform to dir/file on back transform.
                #
                # Yes, this is a BUG, as long as we permit "/" in
                # filenames on Windows and "\" on Unix
                self.PARAM_RELPATH: fi.relative_to(path_info).as_posix(),
            }
            for fi in file_infos
        ]

        # Sorting the list by path to ensure reproducibility
        return sorted(result, key=itemgetter(self.PARAM_RELPATH))

    def get_dir_checksum(self, path_info):
        dir_info = self._collect_dir(path_info)
        checksum, tmp_info = self._get_dir_info_checksum(dir_info)
        new_info = self.cache.checksum_to_path_info(checksum)
        if self.cache.changed_cache_file(checksum):
            self.cache.makedirs(new_info.parent)
            self.cache.move(tmp_info, new_info)

        self.state.save(path_info, checksum)
        self.state.save(new_info, checksum)

        return checksum

    def _get_dir_info_checksum(self, dir_info):
        tmp = tempfile.NamedTemporaryFile(delete=False).name
        with open(tmp, "w+") as fobj:
            json.dump(dir_info, fobj, sort_keys=True)

        from_info = PathInfo(tmp)
        to_info = self.cache.path_info / tmp_fname("")
        self.cache.upload(from_info, to_info, no_progress_bar=True)

        checksum = self.get_file_checksum(to_info) + self.CHECKSUM_DIR_SUFFIX
        return checksum, to_info

    def get_dir_cache(self, checksum):
        assert checksum

        dir_info = self._dir_info.get(checksum)
        if dir_info:
            return dir_info

        try:
            dir_info = self.load_dir_cache(checksum)
        except DirCacheError:
            dir_info = []

        self._dir_info[checksum] = dir_info
        return dir_info

    def load_dir_cache(self, checksum):
        path_info = self.checksum_to_path_info(checksum)

        try:
            with self.cache.open(path_info, "r") as fobj:
                d = json.load(fobj)
        except (ValueError, FileNotFoundError) as exc:
            raise DirCacheError(checksum, cause=exc)

        if not isinstance(d, list):
            msg = "dir cache file format error '{}' [skipping the file]"
            logger.error(msg.format(relpath(path_info)))
            return []

        for info in d:
            # NOTE: here is a BUG, see comment to .as_posix() below
            relative_path = PathInfo.from_posix(info[self.PARAM_RELPATH])
            info[self.PARAM_RELPATH] = relative_path.fspath

        return d

    @classmethod
    def is_dir_checksum(cls, checksum):
        return checksum.endswith(cls.CHECKSUM_DIR_SUFFIX)

    def get_checksum(self, path_info):
        assert path_info.scheme == self.scheme

        if not self.exists(path_info):
            return None

        checksum = self.state.get(path_info)

        # If we have dir checksum in state db, but dir cache file is lost,
        # then we need to recollect the dir via .get_dir_checksum() call below,
        # see https://github.com/iterative/dvc/issues/2219 for context
        if (
            checksum
            and self.is_dir_checksum(checksum)
            and not self.exists(self.cache.checksum_to_path_info(checksum))
        ):
            checksum = None

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

        actual = self.get_checksum(path_info)
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
        self._link(from_info, to_info, self.cache_types)

    def _link(self, from_info, to_info, link_types):
        assert self.isfile(from_info)

        self.makedirs(to_info.parent)

        self._try_links(from_info, to_info, link_types)

    @slow_link_guard
    def _try_links(self, from_info, to_info, link_types):
        while link_types:
            link_method = getattr(self, link_types[0])
            try:
                self._do_link(from_info, to_info, link_method)
                self.cache_type_confirmed = True
                return

            except DvcException as exc:
                msg = "Cache type '{}' is not supported: {}"
                logger.debug(msg.format(link_types[0], str(exc)))
                del link_types[0]

        raise DvcException("no possible cache types left to try out.")

    def _do_link(self, from_info, to_info, link_method):
        if self.exists(to_info):
            raise DvcException("Link '{}' already exists!".format(to_info))

        link_method(from_info, to_info)

        if self.protected:
            self.protect(to_info)

        msg = "Created {}'{}': {} -> {}".format(
            "protected " if self.protected else "",
            self.cache_types[0],
            from_info,
            to_info,
        )
        logger.debug(msg)

    def _save_file(self, path_info, checksum, save_link=True):
        assert checksum

        cache_info = self.checksum_to_path_info(checksum)
        if self.changed_cache(checksum):
            self.move(path_info, cache_info)
            self.link(cache_info, path_info)
        elif self.iscopy(path_info) and self._cache_is_copy(path_info):
            # Default relink procedure involves unneeded copy
            if self.protected:
                self.protect(path_info)
            else:
                self.unprotect(path_info)
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

    def _cache_is_copy(self, path_info):
        """Checks whether cache uses copies."""
        if self.cache_type_confirmed:
            return self.cache_types[0] == "copy"

        if set(self.cache_types) <= {"copy"}:
            return True

        workspace_file = path_info.with_name("." + uuid())
        test_cache_file = self.path_info / ".cache_type_test_file"
        if not self.exists(test_cache_file):
            with self.open(test_cache_file, "wb") as fobj:
                fobj.write(bytes(1))
        try:
            self.link(test_cache_file, workspace_file)
        finally:
            self.remove(workspace_file)
            self.remove(test_cache_file)

        self.cache_type_confirmed = True
        return self.cache_types[0] == "copy"

    def _save_dir(self, path_info, checksum):
        cache_info = self.checksum_to_path_info(checksum)
        dir_info = self.get_dir_cache(checksum)

        for entry in dir_info:
            entry_info = path_info / entry[self.PARAM_RELPATH]
            entry_checksum = entry[self.PARAM_CHECKSUM]
            self._save_file(entry_info, entry_checksum, save_link=False)

        self.state.save_link(path_info)
        self.state.save(cache_info, checksum)
        self.state.save(path_info, checksum)

    def is_empty(self, path_info):
        return False

    def isfile(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return True

    def isdir(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return False

    def iscopy(self, path_info):
        """Check if this file is an independent copy."""
        return False  # We can't be sure by default

    def walk_files(self, path_info):
        """Return a generator with `PathInfo`s to all the files"""
        raise NotImplementedError

    @staticmethod
    def protect(path_info):
        pass

    def save(self, path_info, checksum_info):
        if path_info.scheme != self.scheme:
            raise RemoteActionNotImplemented(
                "save {} -> {}".format(path_info.scheme, self.scheme),
                self.scheme,
            )

        checksum = checksum_info[self.PARAM_CHECKSUM]
        self._save(path_info, checksum)

    def _save(self, path_info, checksum):
        to_info = self.checksum_to_path_info(checksum)
        logger.debug("Saving '{}' to '{}'.".format(path_info, to_info))
        if self.isdir(path_info):
            self._save_dir(path_info, checksum)
            return
        self._save_file(path_info, checksum)

    def _handle_transfer_exception(
        self, from_info, to_info, exception, operation
    ):
        if isinstance(exception, OSError) and exception.errno == errno.EMFILE:
            raise exception

        msg = "failed to {} '{}' to '{}'".format(operation, from_info, to_info)
        logger.exception(msg)
        return 1

    def upload(self, from_info, to_info, name=None, no_progress_bar=False):
        if not hasattr(self, "_upload"):
            raise RemoteActionNotImplemented("upload", self.scheme)

        if to_info.scheme != self.scheme:
            raise NotImplementedError

        if from_info.scheme != "local":
            raise NotImplementedError

        logger.debug("Uploading '{}' to '{}'".format(from_info, to_info))

        name = name or from_info.name

        try:
            self._upload(
                from_info.fspath,
                to_info,
                name=name,
                no_progress_bar=no_progress_bar,
            )
        except Exception as e:
            return self._handle_transfer_exception(
                from_info, to_info, e, "upload"
            )

        return 0

    def download(
        self,
        from_info,
        to_info,
        name=None,
        no_progress_bar=False,
        file_mode=None,
        dir_mode=None,
    ):
        if not hasattr(self, "_download"):
            raise RemoteActionNotImplemented("download", self.scheme)

        if from_info.scheme != self.scheme:
            raise NotImplementedError

        if to_info.scheme == self.scheme != "local":
            self.copy(from_info, to_info)
            return 0

        if to_info.scheme != "local":
            raise NotImplementedError

        if self.isdir(from_info):
            return self._download_dir(
                from_info, to_info, name, no_progress_bar, file_mode, dir_mode
            )
        return self._download_file(
            from_info, to_info, name, no_progress_bar, file_mode, dir_mode
        )

    def _download_dir(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        from_infos = list(self.walk_files(from_info))
        to_infos = (
            to_info / info.relative_to(from_info) for info in from_infos
        )

        with ThreadPoolExecutor(max_workers=self.JOBS) as executor:
            download_files = partial(
                self._download_file,
                name=name,
                no_progress_bar=True,
                file_mode=file_mode,
                dir_mode=dir_mode,
            )
            futures = executor.map(download_files, from_infos, to_infos)
            with Tqdm(
                futures,
                total=len(from_infos),
                desc="Downloading directory",
                unit="Files",
                disable=no_progress_bar,
            ) as futures:
                return sum(futures)

    def _download_file(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        makedirs(to_info.parent, exist_ok=True, mode=dir_mode)

        logger.debug("Downloading '{}' to '{}'".format(from_info, to_info))
        name = name or to_info.name

        tmp_file = tmp_fname(to_info)

        try:
            self._download(
                from_info, tmp_file, name=name, no_progress_bar=no_progress_bar
            )
        except Exception as e:
            return self._handle_transfer_exception(
                from_info, to_info, e, "download"
            )

        move(tmp_file, to_info, mode=file_mode)

        return 0

    def open(self, path_info, mode="r", encoding=None):
        if hasattr(self, "_generate_download_url"):
            get_url = partial(self._generate_download_url, path_info)
            return open_url(get_url, mode=mode, encoding=encoding)

        raise RemoteActionNotImplemented("open", self.scheme)

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def move(self, from_info, to_info):
        self.copy(from_info, to_info)
        self.remove(from_info)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

    def symlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info, to_info):
        raise RemoteActionNotImplemented("reflink", self.scheme)

    def exists(self, path_info):
        raise NotImplementedError

    def path_to_checksum(self, path):
        parts = self.path_cls(path).parts[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError("Bad cache file path")

        return "".join(parts)

    def checksum_to_path_info(self, checksum):
        return self.path_info / checksum[0:2] / checksum[2:]

    def list_cache_paths(self):
        raise NotImplementedError

    def all(self):
        # NOTE: The list might be way too big(e.g. 100M entries, md5 for each
        # is 32 bytes, so ~3200Mb list) and we don't really need all of it at
        # the same time, so it makes sense to use a generator to gradually
        # iterate over it, without keeping all of it in memory.
        for path in self.list_cache_paths():
            try:
                yield self.path_to_checksum(path)
            except ValueError:
                # We ignore all the non-cache looking files
                pass

    def gc(self, named_cache):
        used = self.extract_used_local_checksums(named_cache)

        if self.scheme != "":
            used.update(named_cache[self.scheme])

        removed = False
        for checksum in self.all():
            if checksum in used:
                continue
            path_info = self.checksum_to_path_info(checksum)
            self.remove(path_info)
            removed = True
        return removed

    def changed_cache_file(self, checksum):
        """Compare the given checksum with the (corresponding) actual one.

        - Use `State` as a cache for computed checksums
            + The entries are invalidated by taking into account the following:
                * mtime
                * inode
                * size
                * checksum

        - Remove the file from cache if it doesn't match the actual checksum
        """
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

    def _changed_dir_cache(self, checksum):
        if self.changed_cache_file(checksum):
            return True

        if not self._changed_unpacked_dir(checksum):
            return False

        for entry in self.get_dir_cache(checksum):
            entry_checksum = entry[self.PARAM_CHECKSUM]
            if self.changed_cache_file(entry_checksum):
                return True

        self._update_unpacked_dir(checksum)
        return False

    def changed_cache(self, checksum):
        if self.is_dir_checksum(checksum):
            return self._changed_dir_cache(checksum)
        return self.changed_cache_file(checksum)

    def cache_exists(self, checksums, jobs=None, name=None):
        """Check if the given checksums are stored in the remote.

        There are two ways of performing this check:

        - Traverse: Get a list of all the files in the remote
            (traversing the cache directory) and compare it with
            the given checksums.

        - No traverse: For each given checksum, run the `exists`
            method and filter the checksums that aren't on the remote.
            This is done in parallel threads.
            It also shows a progress bar when performing the check.

        The reason for such an odd logic is that most of the remotes
        take much shorter time to just retrieve everything they have under
        a certain prefix (e.g. s3, gs, ssh, hdfs). Other remotes that can
        check if particular file exists much quicker, use their own
        implementation of cache_exists (see ssh, local).

        Returns:
            A list with checksums that were found in the remote
        """
        if not self.no_traverse:
            return list(set(checksums) & set(self.all()))

        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(checksums),
            unit="file",
        ) as pbar:

            def exists_with_progress(path_info):
                ret = self.exists(path_info)
                pbar.update_desc(str(path_info))
                return ret

            with ThreadPoolExecutor(max_workers=jobs or self.JOBS) as executor:
                path_infos = map(self.checksum_to_path_info, checksums)
                in_remote = executor.map(exists_with_progress, path_infos)
                ret = list(itertools.compress(checksums, in_remote))
                return ret

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
        """The file is changed we need to checkout a new copy"""
        cache_info = self.checksum_to_path_info(checksum)
        if self.exists(path_info):
            msg = "data '{}' exists. Removing before checkout."
            logger.warning(msg.format(str(path_info)))
            self.safe_remove(path_info, force=force)

        self.link(cache_info, path_info)
        self.state.save_link(path_info)
        self.state.save(path_info, checksum)
        if progress_callback:
            progress_callback(str(path_info))

    def makedirs(self, path_info):
        """Optional: Implement only if the remote needs to create
        directories before copying/linking/moving data
        """
        pass

    def _checkout_dir(
        self, path_info, checksum, force, progress_callback=None, relink=False
    ):
        # Create dir separately so that dir is created
        # even if there are no files in it
        if not self.exists(path_info):
            self.makedirs(path_info)

        dir_info = self.get_dir_cache(checksum)

        logger.debug("Linking directory '{}'.".format(path_info))

        for entry in dir_info:
            relative_path = entry[self.PARAM_RELPATH]
            entry_checksum = entry[self.PARAM_CHECKSUM]
            entry_cache_info = self.checksum_to_path_info(entry_checksum)
            entry_info = path_info / relative_path

            entry_checksum_info = {self.PARAM_CHECKSUM: entry_checksum}
            if relink or self.changed(entry_info, entry_checksum_info):
                self.safe_remove(entry_info, force=force)
                self.link(entry_cache_info, entry_info)
                self.state.save(entry_info, entry_checksum)
            if progress_callback:
                progress_callback(str(entry_info))

        self._remove_redundant_files(path_info, dir_info, force)

        self.state.save_link(path_info)
        self.state.save(path_info, checksum)

    def _remove_redundant_files(self, path_info, dir_info, force):
        existing_files = set(self.walk_files(path_info))

        needed_files = {
            path_info / entry[self.PARAM_RELPATH] for entry in dir_info
        }

        for path in existing_files - needed_files:
            self.safe_remove(path, force)

    def checkout(
        self,
        path_info,
        checksum_info,
        force=False,
        progress_callback=None,
        relink=False,
    ):
        if path_info.scheme not in ["local", self.scheme]:
            raise NotImplementedError

        checksum = checksum_info.get(self.PARAM_CHECKSUM)
        failed = None
        skip = False
        if not checksum:
            logger.warning(
                "No checksum info found for '{}'. "
                "It won't be created.".format(str(path_info))
            )
            self.safe_remove(path_info, force=force)
            failed = path_info

        elif not relink and not self.changed(path_info, checksum_info):
            msg = "Data '{}' didn't change."
            logger.debug(msg.format(str(path_info)))
            skip = True

        elif self.changed_cache(checksum):
            msg = "Cache '{}' not found. File '{}' won't be created."
            logger.warning(msg.format(checksum, str(path_info)))
            self.safe_remove(path_info, force=force)
            failed = path_info

        if failed or skip:
            if progress_callback:
                progress_callback(
                    str(path_info), self.get_files_number(checksum)
                )
            return failed

        msg = "Checking out '{}' with cache '{}'."
        logger.debug(msg.format(str(path_info), checksum))

        self._checkout(path_info, checksum, force, progress_callback, relink)
        return None

    def _checkout(
        self,
        path_info,
        checksum,
        force=False,
        progress_callback=None,
        relink=False,
    ):
        if not self.is_dir_checksum(checksum):
            return self._checkout_file(
                path_info, checksum, force, progress_callback=progress_callback
            )
        return self._checkout_dir(
            path_info, checksum, force, progress_callback, relink
        )

    def get_files_number(self, checksum):
        if not checksum:
            return 0

        if self.is_dir_checksum(checksum):
            return len(self.get_dir_cache(checksum))

        return 1

    @staticmethod
    def unprotect(path_info):
        pass

    def _get_unpacked_dir_names(self, checksums):
        return set()

    def extract_used_local_checksums(self, named_cache):
        used = set(named_cache["local"])
        unpacked = self._get_unpacked_dir_names(used)
        return used | unpacked

    def _changed_unpacked_dir(self, checksum):
        return True

    def _update_unpacked_dir(self, checksum):
        pass
