import errno
import logging
import os
import stat
from concurrent.futures import as_completed, ThreadPoolExecutor
from functools import partial

from funcy import cached_property, concat

from shortuuid import uuid

from dvc.compat import fspath_py35
from dvc.exceptions import DvcException, DownloadError, UploadError
from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.remote.base import (
    index_locked,
    BaseRemote,
    STATUS_MAP,
    STATUS_DELETED,
    STATUS_MISSING,
    STATUS_NEW,
)
from dvc.remote.index import RemoteIndexNoop
from dvc.scheme import Schemes
from dvc.scm.tree import is_working_tree
from dvc.system import System
from dvc.utils import file_md5, relpath, tmp_fname
from dvc.utils.fs import copyfile, move, makedirs, remove, walk_files

logger = logging.getLogger(__name__)


class LocalRemote(BaseRemote):
    scheme = Schemes.LOCAL
    path_cls = PathInfo
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2
    INDEX_CLS = RemoteIndexNoop

    UNPACKED_DIR_SUFFIX = ".unpacked"

    DEFAULT_CACHE_TYPES = ["reflink", "copy"]

    CACHE_MODE = 0o444
    SHARED_MODE_MAP = {None: (0o644, 0o755), "group": (0o664, 0o775)}

    def __init__(self, repo, config):
        super().__init__(repo, config)
        self.cache_dir = config.get("url")
        self._dir_info = {}

    @property
    def state(self):
        return self.repo.state

    @property
    def cache_dir(self):
        return self.path_info.fspath if self.path_info else None

    @cache_dir.setter
    def cache_dir(self, value):
        self.path_info = PathInfo(value) if value else None

    @classmethod
    def supported(cls, config):
        return True

    @cached_property
    def cache_path(self):
        return os.path.abspath(self.cache_dir)

    def checksum_to_path(self, checksum):
        return os.path.join(self.cache_path, checksum[0:2], checksum[2:])

    def list_cache_paths(self, prefix=None, progress_callback=None):
        assert self.path_info is not None
        if prefix:
            path_info = self.path_info / prefix[:2]
        else:
            path_info = self.path_info
        if progress_callback:
            for path in walk_files(path_info):
                progress_callback()
                yield path
        else:
            yield from walk_files(path_info)

    def get(self, md5):
        if not md5:
            return None

        return self.checksum_to_path_info(md5).url

    def exists(self, path_info):
        assert is_working_tree(self.repo.tree)
        assert isinstance(path_info, str) or path_info.scheme == "local"
        return self.repo.tree.exists(fspath_py35(path_info))

    def makedirs(self, path_info):
        makedirs(path_info, exist_ok=True, mode=self._dir_mode)

    def already_cached(self, path_info):
        assert path_info.scheme in ["", "local"]

        current_md5 = self.get_checksum(path_info)

        if not current_md5:
            return False

        return not self.changed_cache(current_md5)

    def _verify_link(self, path_info, link_type):
        if link_type == "hardlink" and self.getsize(path_info) == 0:
            return

        super()._verify_link(path_info, link_type)

    def is_empty(self, path_info):
        path = path_info.fspath

        if self.isfile(path_info) and os.path.getsize(path) == 0:
            return True

        if self.isdir(path_info) and len(os.listdir(path)) == 0:
            return True

        return False

    @staticmethod
    def isfile(path_info):
        return os.path.isfile(fspath_py35(path_info))

    @staticmethod
    def isdir(path_info):
        return os.path.isdir(fspath_py35(path_info))

    def iscopy(self, path_info):
        return not (
            System.is_symlink(path_info) or System.is_hardlink(path_info)
        )

    @staticmethod
    def getsize(path_info):
        return os.path.getsize(fspath_py35(path_info))

    def walk_files(self, path_info):
        assert is_working_tree(self.repo.tree)

        for fname in self.repo.tree.walk_files(path_info):
            yield PathInfo(fname)

    def get_file_checksum(self, path_info):
        return file_md5(path_info)[0]

    def remove(self, path_info):
        if isinstance(path_info, PathInfo):
            if path_info.scheme != "local":
                raise NotImplementedError
            path = path_info.fspath
        else:
            path = path_info

        if self.exists(path):
            remove(path)

    def move(self, from_info, to_info, mode=None):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        self.makedirs(to_info.parent)

        if mode is None:
            if self.isfile(from_info):
                mode = self._file_mode
            else:
                mode = self._dir_mode

        move(from_info, to_info, mode=mode)

    def copy(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname(to_info.name)
        try:
            System.copy(from_info, tmp_info)
            os.chmod(fspath_py35(tmp_info), self._file_mode)
            os.rename(fspath_py35(tmp_info), fspath_py35(to_info))
        except Exception:
            self.remove(tmp_info)
            raise

    @staticmethod
    def symlink(from_info, to_info):
        System.symlink(from_info, to_info)

    @staticmethod
    def is_symlink(path_info):
        return System.is_symlink(path_info)

    def hardlink(self, from_info, to_info):
        # If there are a lot of empty files (which happens a lot in datasets),
        # and the cache type is `hardlink`, we might reach link limits and
        # will get something like: `too many links error`
        #
        # This is because all those empty files will have the same checksum
        # (i.e. 68b329da9893e34099c7d8ad5cb9c940), therefore, they will be
        # linked to the same file in the cache.
        #
        # From https://en.wikipedia.org/wiki/Hard_link
        #   * ext4 limits the number of hard links on a file to 65,000
        #   * Windows with NTFS has a limit of 1024 hard links on a file
        #
        # That's why we simply create an empty file rather than a link.
        if self.getsize(from_info) == 0:
            self.open(to_info, "w").close()

            logger.debug(
                "Created empty file: {src} -> {dest}".format(
                    src=str(from_info), dest=str(to_info)
                )
            )
            return

        System.hardlink(from_info, to_info)

    @staticmethod
    def is_hardlink(path_info):
        return System.is_hardlink(path_info)

    def reflink(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname(to_info.name)
        System.reflink(from_info, tmp_info)
        # NOTE: reflink has its own separate inode, so you can set permissions
        # that are different from the source.
        os.chmod(fspath_py35(tmp_info), self._file_mode)
        os.rename(fspath_py35(tmp_info), fspath_py35(to_info))

    def cache_exists(self, checksums, jobs=None, name=None):
        return [
            checksum
            for checksum in Tqdm(
                checksums,
                unit="file",
                desc="Querying "
                + ("cache in " + name if name else "local cache"),
            )
            if not self.changed_cache_file(checksum)
        ]

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        makedirs(to_info.parent, exist_ok=True)

        tmp_file = tmp_fname(to_info)
        copyfile(
            from_file, tmp_file, name=name, no_progress_bar=no_progress_bar
        )
        os.rename(tmp_file, fspath_py35(to_info))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        copyfile(
            from_info, to_file, no_progress_bar=no_progress_bar, name=name
        )

    @staticmethod
    def open(path_info, mode="r", encoding=None):
        return open(fspath_py35(path_info), mode=mode, encoding=encoding)

    @index_locked
    def status(
        self,
        named_cache,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        # Return flattened dict containing all status info
        dir_status, file_status, _ = self._status(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )
        return dict(dir_status, **file_status)

    def _status(
        self,
        named_cache,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        """Return a tuple of (dir_status_info, file_status_info, dir_contents).

        dir_status_info contains status for .dir files, file_status_info
        contains status for all other files, and dir_contents is a dict of
        {dir_checksum: set(file_checksum, ...)} which can be used to map
        a .dir file to its file contents.
        """
        logger.debug(
            "Preparing to collect status from {}".format(remote.path_info)
        )
        md5s = set(named_cache.scheme_keys(self.scheme))

        logger.debug("Collecting information from local cache...")
        local_exists = frozenset(
            self.cache_exists(md5s, jobs=jobs, name=self.cache_dir)
        )

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existence of
        # those files.
        if download and local_exists == md5s:
            remote_exists = local_exists
        else:
            logger.debug("Collecting information from remote cache...")
            remote_exists = set()
            dir_md5s = set(named_cache.dir_keys(self.scheme))
            if dir_md5s:
                remote_exists.update(
                    self._indexed_dir_checksums(named_cache, remote, dir_md5s)
                )
                md5s.difference_update(remote_exists)
            if md5s:
                remote_exists.update(
                    remote.cache_exists(
                        md5s, jobs=jobs, name=str(remote.path_info)
                    )
                )
        return self._make_status(
            named_cache, show_checksums, local_exists, remote_exists
        )

    def _make_status(
        self, named_cache, show_checksums, local_exists, remote_exists
    ):
        def make_names(checksum, names):
            return {"name": checksum if show_checksums else " ".join(names)}

        dir_status = {}
        file_status = {}
        dir_contents = {}
        for checksum, item in named_cache[self.scheme].items():
            if item.children:
                dir_status[checksum] = make_names(checksum, item.names)
                dir_contents[checksum] = set()
                for child_checksum, child in item.children.items():
                    file_status[child_checksum] = make_names(
                        child_checksum, child.names
                    )
                    dir_contents[checksum].add(child_checksum)
            else:
                file_status[checksum] = make_names(checksum, item.names)

        self._fill_statuses(dir_status, local_exists, remote_exists)
        self._fill_statuses(file_status, local_exists, remote_exists)

        self._log_missing_caches(dict(dir_status, **file_status))

        return dir_status, file_status, dir_contents

    def _indexed_dir_checksums(self, named_cache, remote, dir_md5s):
        # Validate our index by verifying all indexed .dir checksums
        # still exist on the remote
        indexed_dirs = set(remote.index.dir_checksums())
        indexed_dir_exists = set()
        if indexed_dirs:
            indexed_dir_exists.update(
                remote._cache_object_exists(indexed_dirs)
            )
            missing_dirs = indexed_dirs.difference(indexed_dir_exists)
            if missing_dirs:
                logger.debug(
                    "Remote cache missing indexed .dir checksums '{}', "
                    "clearing remote index".format(", ".join(missing_dirs))
                )
                remote.index.clear()

        # Check if non-indexed (new) dir checksums exist on remote
        dir_exists = dir_md5s.intersection(indexed_dir_exists)
        dir_exists.update(remote._cache_object_exists(dir_md5s - dir_exists))

        # If .dir checksum exists on the remote, assume directory contents
        # still exists on the remote
        for dir_checksum in dir_exists:
            file_checksums = list(
                named_cache.child_keys(self.scheme, dir_checksum)
            )
            if dir_checksum not in remote.index:
                logger.debug(
                    "Indexing new .dir '{}' with '{}' nested files".format(
                        dir_checksum, len(file_checksums)
                    )
                )
                remote.index.update([dir_checksum], file_checksums)
            yield dir_checksum
            yield from file_checksums

    @staticmethod
    def _fill_statuses(checksum_info_dir, local_exists, remote_exists):
        # Using sets because they are way faster for lookups
        local = set(local_exists)
        remote = set(remote_exists)

        for md5, info in checksum_info_dir.items():
            status = STATUS_MAP[(md5 in local, md5 in remote)]
            info["status"] = status

    def _get_plans(self, download, remote, status_info, status):
        cache = []
        path_infos = []
        names = []
        checksums = []
        for md5, info in Tqdm(
            status_info.items(), desc="Analysing status", unit="file"
        ):
            if info["status"] == status:
                cache.append(self.checksum_to_path_info(md5))
                path_infos.append(remote.checksum_to_path_info(md5))
                names.append(info["name"])
                checksums.append(md5)

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return from_infos, to_infos, names, checksums

    def _process(
        self,
        named_cache,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.debug(
            "Preparing to {} '{}'".format(
                "download data from" if download else "upload data to",
                remote.path_info,
            )
        )

        if download:
            func = partial(
                remote.download,
                dir_mode=self._dir_mode,
                file_mode=self._file_mode,
            )
            status = STATUS_DELETED
            desc = "Downloading"
        else:
            func = remote.upload
            status = STATUS_NEW
            desc = "Uploading"

        if jobs is None:
            jobs = remote.JOBS

        dir_status, file_status, dir_contents = self._status(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        dir_plans = self._get_plans(download, remote, dir_status, status)
        file_plans = self._get_plans(download, remote, file_status, status)

        total = len(dir_plans[0]) + len(file_plans[0])
        if total == 0:
            return 0

        with Tqdm(total=total, unit="file", desc=desc) as pbar:
            func = pbar.wrap_fn(func)
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                if download:
                    fails = sum(executor.map(func, *dir_plans))
                    fails += sum(executor.map(func, *file_plans))
                else:
                    # for uploads, push files first, and any .dir files last

                    file_futures = {}
                    for from_info, to_info, name, checksum in zip(*file_plans):
                        file_futures[checksum] = executor.submit(
                            func, from_info, to_info, name
                        )
                    dir_futures = {}
                    for from_info, to_info, name, dir_checksum in zip(
                        *dir_plans
                    ):
                        wait_futures = {
                            future
                            for file_checksum, future in file_futures.items()
                            if file_checksum in dir_contents[dir_checksum]
                        }
                        dir_futures[dir_checksum] = executor.submit(
                            self._dir_upload,
                            func,
                            wait_futures,
                            from_info,
                            to_info,
                            name,
                        )
                    fails = sum(
                        future.result()
                        for future in concat(
                            file_futures.values(), dir_futures.values()
                        )
                    )

        if fails:
            if download:
                remote.index.clear()
                raise DownloadError(fails)
            raise UploadError(fails)

        if not download:
            # index successfully pushed dirs
            for dir_checksum, future in dir_futures.items():
                if future.result() == 0:
                    file_checksums = dir_contents[dir_checksum]
                    logger.debug(
                        "Indexing pushed dir '{}' with "
                        "'{}' nested files".format(
                            dir_checksum, len(file_checksums)
                        )
                    )
                    remote.index.update([dir_checksum], file_checksums)

        return len(dir_plans[0]) + len(file_plans[0])

    @staticmethod
    def _dir_upload(func, futures, from_info, to_info, name):
        for future in as_completed(futures):
            if future.result():
                # do not upload this .dir file if any file in this
                # directory failed to upload
                logger.debug(
                    "failed to upload full contents of '{}', "
                    "aborting .dir file upload".format(name)
                )
                logger.error(
                    "failed to upload '{}' to '{}'".format(from_info, to_info)
                )
                return 1
        return func(from_info, to_info, name)

    @index_locked
    def push(self, named_cache, remote, jobs=None, show_checksums=False):
        return self._process(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=False,
        )

    @index_locked
    def pull(self, named_cache, remote, jobs=None, show_checksums=False):
        return self._process(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=True,
        )

    @staticmethod
    def _log_missing_caches(checksum_info_dict):
        missing_caches = [
            (md5, info)
            for md5, info in checksum_info_dict.items()
            if info["status"] == STATUS_MISSING
        ]
        if missing_caches:
            missing_desc = "".join(
                "\nname: {}, md5: {}".format(info["name"], md5)
                for md5, info in missing_caches
            )
            msg = (
                "Some of the cache files do not exist neither locally "
                "nor on remote. Missing cache files: {}".format(missing_desc)
            )
            logger.warning(msg)

    def _unprotect_file(self, path):
        if System.is_symlink(path) or System.is_hardlink(path):
            logger.debug("Unprotecting '{}'".format(path))
            tmp = os.path.join(os.path.dirname(path), "." + uuid())

            # The operations order is important here - if some application
            # would access the file during the process of copyfile then it
            # would get only the part of file. So, at first, the file should be
            # copied with the temporary name, and then original file should be
            # replaced by new.
            copyfile(path, tmp, name="Unprotecting '{}'".format(relpath(path)))
            remove(path)
            os.rename(tmp, path)

        else:
            logger.debug(
                "Skipping copying for '{}', since it is not "
                "a symlink or a hardlink.".format(path)
            )

        os.chmod(path, self._file_mode)

    def _unprotect_dir(self, path):
        assert is_working_tree(self.repo.tree)

        for fname in self.repo.tree.walk_files(path):
            self._unprotect_file(fname)

    def unprotect(self, path_info):
        path = path_info.fspath
        if not os.path.exists(path):
            raise DvcException(
                "can't unprotect non-existing data '{}'".format(path)
            )

        if os.path.isdir(path):
            self._unprotect_dir(path)
        else:
            self._unprotect_file(path)

    def protect(self, path_info):
        path = fspath_py35(path_info)
        mode = self.CACHE_MODE

        try:
            os.chmod(path, mode)
        except OSError as exc:
            # There is nothing we need to do in case of a read-only file system
            if exc.errno == errno.EROFS:
                return

            # In shared cache scenario, we might not own the cache file, so we
            # need to check if cache file is already protected.
            if exc.errno not in [errno.EPERM, errno.EACCES]:
                raise

            actual = stat.S_IMODE(os.stat(path).st_mode)
            if actual != mode:
                raise

    def _get_unpacked_dir_path_info(self, checksum):
        info = self.checksum_to_path_info(checksum)
        return info.with_name(info.name + self.UNPACKED_DIR_SUFFIX)

    def _remove_unpacked_dir(self, checksum):
        path_info = self._get_unpacked_dir_path_info(checksum)
        self.remove(path_info)

    def _path_info_changed(self, path_info):
        if self.exists(path_info) and self.state.get(path_info):
            return False
        return True

    def _update_unpacked_dir(self, checksum):
        unpacked_dir_info = self._get_unpacked_dir_path_info(checksum)

        if not self._path_info_changed(unpacked_dir_info):
            return

        self.remove(unpacked_dir_info)

        try:
            dir_info = self.get_dir_cache(checksum)
            self._create_unpacked_dir(checksum, dir_info, unpacked_dir_info)
        except DvcException:
            logger.warning("Could not create '{}'".format(unpacked_dir_info))

            self.remove(unpacked_dir_info)

    def _create_unpacked_dir(self, checksum, dir_info, unpacked_dir_info):
        self.makedirs(unpacked_dir_info)

        for entry in Tqdm(dir_info, desc="Creating unpacked dir", unit="file"):
            entry_cache_info = self.checksum_to_path_info(
                entry[self.PARAM_CHECKSUM]
            )
            relative_path = entry[self.PARAM_RELPATH]
            # In shared cache mode some cache files might not be owned by the
            # user, so we need to use symlinks because, unless
            # /proc/sys/fs/protected_hardlinks is disabled, the user is not
            # allowed to create hardlinks to files that he doesn't own.
            link_types = ["hardlink", "symlink"]
            self._link(
                entry_cache_info, unpacked_dir_info / relative_path, link_types
            )

        self.state.save(unpacked_dir_info, checksum)

    def _changed_unpacked_dir(self, checksum):
        status_unpacked_dir_info = self._get_unpacked_dir_path_info(checksum)

        return not self.state.get(status_unpacked_dir_info)

    def _get_unpacked_dir_names(self, checksums):
        unpacked = set()
        for c in checksums:
            if self.is_dir_checksum(c):
                unpacked.add(c + self.UNPACKED_DIR_SUFFIX)
        return unpacked

    def is_protected(self, path_info):
        if not self.exists(path_info):
            return False

        mode = os.stat(fspath_py35(path_info)).st_mode

        return stat.S_IMODE(mode) == self.CACHE_MODE
