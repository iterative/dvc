from __future__ import unicode_literals

import errno
import logging
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from shortuuid import uuid

from dvc.config import Config
from dvc.exceptions import DownloadError
from dvc.exceptions import DvcException
from dvc.exceptions import UploadError
from dvc.path_info import PathInfo
from dvc.progress import Tqdm
from dvc.remote.base import RemoteBASE
from dvc.remote.base import STATUS_DELETED
from dvc.remote.base import STATUS_MAP
from dvc.remote.base import STATUS_MISSING
from dvc.remote.base import STATUS_NEW
from dvc.scheme import Schemes
from dvc.system import System
from dvc.utils import copyfile
from dvc.utils import file_md5
from dvc.utils import makedirs
from dvc.utils import relpath
from dvc.utils import tmp_fname
from dvc.utils import walk_files
from dvc.utils.compat import fspath_py35
from dvc.utils.compat import open
from dvc.utils.compat import str
from dvc.utils.fs import move
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


class RemoteLOCAL(RemoteBASE):
    scheme = Schemes.LOCAL
    path_cls = PathInfo
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"

    UNPACKED_DIR_SUFFIX = ".unpacked"

    DEFAULT_CACHE_TYPES = ["reflink", "copy"]

    SHARED_MODE_MAP = {None: (0o644, 0o755), "group": (0o664, 0o775)}

    def __init__(self, repo, config):
        super(RemoteLOCAL, self).__init__(repo, config)
        self.protected = config.get(Config.SECTION_CACHE_PROTECTED, False)

        shared = config.get(Config.SECTION_CACHE_SHARED)
        self._file_mode, self._dir_mode = self.SHARED_MODE_MAP[shared]

        if self.protected:
            # cache files are set to be read-only for everyone
            self._file_mode = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH

        cache_dir = config.get(Config.SECTION_REMOTE_URL)

        if cache_dir is not None and not os.path.isabs(cache_dir):
            cwd = config[Config.PRIVATE_CWD]
            cache_dir = os.path.abspath(os.path.join(cwd, cache_dir))

        self.path_info = PathInfo(cache_dir) if cache_dir else None
        self._dir_info = {}

    @property
    def state(self):
        return self.repo.state

    @property
    def cache_dir(self):
        return self.path_info.fspath if self.path_info else None

    @classmethod
    def supported(cls, config):
        return True

    def list_cache_paths(self):
        assert self.path_info is not None
        return walk_files(self.path_info, None)

    def get(self, md5):
        if not md5:
            return None

        return self.checksum_to_path_info(md5).url

    @staticmethod
    def exists(path_info):
        assert path_info.scheme == "local"
        return os.path.lexists(fspath_py35(path_info))

    def makedirs(self, path_info):
        makedirs(path_info, exist_ok=True, mode=self._dir_mode)

    def already_cached(self, path_info):
        assert path_info.scheme in ["", "local"]

        current_md5 = self.get_checksum(path_info)

        if not current_md5:
            return False

        return not self.changed_cache(current_md5)

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
        for fname in walk_files(path_info, self.repo.dvcignore):
            yield PathInfo(fname)

    def get_file_checksum(self, path_info):
        return file_md5(path_info)[0]

    def remove(self, path_info):
        if path_info.scheme != "local":
            raise NotImplementedError

        if self.exists(path_info):
            remove(path_info.fspath)

    def move(self, from_info, to_info):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        self.makedirs(to_info.parent)

        if self.isfile(from_info):
            mode = self._file_mode
        else:
            mode = self._dir_mode

        move(from_info, to_info, mode=mode)

    def copy(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname(to_info.name)
        try:
            System.copy(from_info, tmp_info)
            os.rename(fspath_py35(tmp_info), fspath_py35(to_info))
        except Exception:
            self.remove(tmp_info)
            raise

    @staticmethod
    def symlink(from_info, to_info):
        System.symlink(from_info, to_info)

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
    def reflink(from_info, to_info):
        System.reflink(from_info, to_info)

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

    def status(
        self,
        named_cache,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.debug(
            "Preparing to collect status from {}".format(remote.path_info)
        )
        md5s = list(named_cache[self.scheme])

        logger.debug("Collecting information from local cache...")
        local_exists = self.cache_exists(md5s, jobs=jobs, name=self.cache_dir)

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existence of
        # those files.
        if download and sorted(local_exists) == sorted(md5s):
            remote_exists = local_exists
        else:
            logger.debug("Collecting information from remote cache...")
            remote_exists = list(
                remote.cache_exists(
                    md5s, jobs=jobs, name=str(remote.path_info)
                )
            )

        ret = {
            checksum: {"name": checksum if show_checksums else " ".join(names)}
            for checksum, names in named_cache[self.scheme].items()
        }
        self._fill_statuses(ret, local_exists, remote_exists)

        self._log_missing_caches(ret)

        return ret

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
        for md5, info in Tqdm(
            status_info.items(), desc="Analysing status", unit="file"
        ):
            if info["status"] == status:
                cache.append(self.checksum_to_path_info(md5))
                path_infos.append(remote.checksum_to_path_info(md5))
                names.append(info["name"])

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return from_infos, to_infos, names

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
        else:
            func = remote.upload
            status = STATUS_NEW

        if jobs is None:
            jobs = remote.JOBS

        status_info = self.status(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        plans = self._get_plans(download, remote, status_info, status)

        if len(plans[0]) == 0:
            return 0

        if jobs > 1:
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                fails = sum(executor.map(func, *plans))
        else:
            fails = sum(map(func, *plans))

        if fails:
            if download:
                raise DownloadError(fails)
            raise UploadError(fails)

        return len(plans[0])

    def push(self, named_cache, remote, jobs=None, show_checksums=False):
        return self._process(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=False,
        )

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

    @staticmethod
    def _unprotect_file(path):
        if System.is_symlink(path) or System.is_hardlink(path):
            logger.debug("Unprotecting '{}'".format(path))
            tmp = os.path.join(os.path.dirname(path), "." + str(uuid()))

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

        os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)

    def _unprotect_dir(self, path):
        for fname in walk_files(path, self.repo.dvcignore):
            RemoteLOCAL._unprotect_file(fname)

    def unprotect(self, path_info):
        path = path_info.fspath
        if not os.path.exists(path):
            raise DvcException(
                "can't unprotect non-existing data '{}'".format(path)
            )

        if os.path.isdir(path):
            self._unprotect_dir(path)
        else:
            RemoteLOCAL._unprotect_file(path)

    @staticmethod
    def protect(path_info):
        path = fspath_py35(path_info)
        mode = stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH

        try:
            os.chmod(path, mode)
        except OSError as exc:
            # In share cache scenario, we might not own the cache file, so we
            # need to check if cache file is already protected.
            if exc.errno not in [errno.EPERM, errno.EACCES]:
                raise

            actual = os.stat(path).st_mode
            if actual & mode != mode:
                raise

    def _get_unpacked_dir_path_info(self, checksum):
        info = self.checksum_to_path_info(checksum)
        return info.with_name(info.name + self.UNPACKED_DIR_SUFFIX)

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
