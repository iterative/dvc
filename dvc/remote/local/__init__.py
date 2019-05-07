from __future__ import unicode_literals

from copy import copy

from dvc.path import BasePathInfo, Schemes
from dvc.path.local import LocalPathInfo
from dvc.remote.local.slow_link_detection import slow_link_guard
from dvc.utils.compat import str, makedirs

import os
import stat
import uuid
import ntpath
import shutil
import posixpath
import logging

from dvc.system import System
from dvc.remote.base import (
    RemoteBase,
    STATUS_MAP,
    STATUS_NEW,
    STATUS_DELETED,
    STATUS_MISSING,
)
from dvc.utils import (
    remove,
    move,
    copyfile,
    to_chunks,
    tmp_fname,
    file_md5,
    walk_files,
)
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.progress import progress
from concurrent.futures import ThreadPoolExecutor

from dvc.utils.fs import get_mtime_and_size, get_inode


logger = logging.getLogger(__name__)


class RemoteLOCAL(RemoteBase):
    scheme = Schemes.LOCAL
    REGEX = r"^(?P<path>.*)$"
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"

    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_TYPE_MAP = {
        "copy": shutil.copyfile,
        "symlink": System.symlink,
        "hardlink": System.hardlink,
        "reflink": System.reflink,
    }

    def __init__(self, repo, config):
        super(RemoteLOCAL, self).__init__(repo, config)
        self.state = self.repo.state if self.repo else None
        self.protected = config.get(Config.SECTION_CACHE_PROTECTED, False)
        storagepath = config.get(Config.SECTION_AWS_STORAGEPATH, None)
        self.cache_dir = config.get(Config.SECTION_REMOTE_URL, storagepath)

        if self.cache_dir is not None and not os.path.isabs(self.cache_dir):
            cwd = config[Config.PRIVATE_CWD]
            self.cache_dir = os.path.abspath(os.path.join(cwd, self.cache_dir))

        types = config.get(Config.SECTION_CACHE_TYPE, None)
        if types:
            if isinstance(types, str):
                types = [t.strip() for t in types.split(",")]
            self.cache_types = types
        else:
            self.cache_types = copy(self.DEFAULT_CACHE_TYPES)

        if self.cache_dir is not None and not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        self._dir_info = {}
        self.path_info = LocalPathInfo()

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = ret.pop(Config.SECTION_AWS_STORAGEPATH, "")
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def url(self):
        return self.cache_dir

    @property
    def prefix(self):
        return self.cache_dir

    def list_cache_paths(self):
        clist = []
        for entry in os.listdir(self.cache_dir):
            subdir = os.path.join(self.cache_dir, entry)
            if not os.path.isdir(subdir):
                continue

            for cache in os.listdir(subdir):
                clist.append(os.path.join(subdir, cache))

        return clist

    def get(self, md5):
        if not md5:
            return None

        return self.checksum_to_path(md5)

    def exists(self, path_info):
        assert isinstance(path_info, BasePathInfo)
        assert path_info.scheme == "local"
        return os.path.lexists(path_info.path)

    def makedirs(self, path_info):
        if not self.exists(path_info):
            os.makedirs(path_info.path)

    @slow_link_guard
    def link(self, cache_info, path_info):
        cache = cache_info.path
        path = path_info.path

        assert os.path.isfile(cache)

        dname = os.path.dirname(path)
        if not os.path.exists(dname):
            os.makedirs(dname)

        # NOTE: just create an empty file for an empty cache
        if os.path.getsize(cache) == 0:
            open(path, "w+").close()

            msg = "Created empty file: {} -> {}".format(cache, path)
            logger.debug(msg)
            return

        i = len(self.cache_types)
        while i > 0:
            try:
                self.CACHE_TYPE_MAP[self.cache_types[0]](cache, path)

                if self.protected:
                    self.protect(path_info)

                msg = "Created {}'{}': {} -> {}".format(
                    "protected " if self.protected else "",
                    self.cache_types[0],
                    cache,
                    path,
                )

                logger.debug(msg)
                return

            except DvcException as exc:
                msg = "Cache type '{}' is not supported: {}"
                logger.debug(msg.format(self.cache_types[0], str(exc)))
                del self.cache_types[0]
                i -= 1

        raise DvcException("no possible cache types left to try out.")

    @property
    def ospath(self):
        if os.name == "nt":
            return ntpath
        return posixpath

    def already_cached(self, path_info):
        assert path_info.scheme in ["", "local"]

        current_md5 = self.get_checksum(path_info)

        if not current_md5:
            return False

        return not self.changed_cache(current_md5)

    def is_empty(self, path_info):
        path = path_info.path

        if self.isfile(path_info) and os.path.getsize(path) == 0:
            return True

        if self.isdir(path_info) and len(os.listdir(path)) == 0:
            return True

        return False

    def isfile(self, path_info):
        return os.path.isfile(path_info.path)

    def isdir(self, path_info):
        return os.path.isdir(path_info.path)

    def walk(self, path_info):
        return os.walk(path_info.path)

    def get_file_checksum(self, path_info):
        return file_md5(path_info.path)[0]

    def remove(self, path_info):
        if path_info.scheme != "local":
            raise NotImplementedError

        remove(path_info.path)

    def move(self, from_info, to_info):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        inp = from_info.path
        outp = to_info.path

        # moving in two stages to make the whole operation atomic in
        # case inp and outp are in different filesystems and actual
        # physical copying of data is happening
        tmp = "{}.{}".format(outp, str(uuid.uuid4()))
        move(inp, tmp)
        move(tmp, outp)

    def cache_exists(self, md5s):
        assert isinstance(md5s, list)
        return list(filter(lambda md5: not self.changed_cache_file(md5), md5s))

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info.scheme != "local":
                raise NotImplementedError

            if from_info.scheme != "local":
                raise NotImplementedError

            logger.debug(
                "Uploading '{}' to '{}'".format(from_info.path, to_info.path)
            )

            if not name:
                name = os.path.basename(from_info.path)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)
            tmp_file = tmp_fname(to_info.path)

            try:
                copyfile(
                    from_info.path,
                    tmp_file,
                    name=name,
                    no_progress_bar=no_progress_bar,
                )
                os.rename(tmp_file, to_info.path)
            except Exception:
                logger.exception(
                    "failed to upload '{}' to '{}'".format(
                        from_info.path, to_info.path
                    )
                )

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != "local":
                raise NotImplementedError

            if to_info.scheme != "local":
                raise NotImplementedError

            logger.debug(
                "Downloading '{}' to '{}'".format(from_info.path, to_info.path)
            )

            if not name:
                name = os.path.basename(to_info.path)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)
            tmp_file = tmp_fname(to_info.path)
            try:
                copyfile(
                    from_info.path,
                    tmp_file,
                    no_progress_bar=no_progress_bar,
                    name=name,
                )

                move(tmp_file, to_info.path)
            except Exception:
                logger.exception(
                    "failed to download '{}' to '{}'".format(
                        from_info.path, to_info.path
                    )
                )

                continue

    def _group(self, checksum_infos, show_checksums=False):
        by_md5 = {}

        for info in checksum_infos:
            md5 = info[self.PARAM_CHECKSUM]

            if show_checksums:
                by_md5[md5] = {"name": md5}
                continue

            name = info[self.PARAM_PATH]
            branch = info.get("branch")
            if branch:
                name += "({})".format(branch)

            if md5 not in by_md5.keys():
                by_md5[md5] = {"name": name}
            else:
                by_md5[md5]["name"] += " " + name

        return by_md5

    def status(
        self,
        checksum_infos,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.info("Preparing to collect status from {}".format(remote.url))
        title = "Collecting information"

        ret = {}

        progress.set_n_total(1)
        progress.update_target(title, 0, 100)

        progress.update_target(title, 10, 100)

        ret = self._group(checksum_infos, show_checksums=show_checksums)
        md5s = list(ret.keys())

        progress.update_target(title, 30, 100)

        local_exists = self.cache_exists(md5s)

        progress.update_target(title, 40, 100)

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existance of
        # those files.
        if download and sorted(local_exists) == sorted(md5s):
            remote_exists = local_exists
        else:
            remote_exists = list(remote.cache_exists(md5s))

        progress.update_target(title, 90, 100)

        progress.finish_target(title)

        self._fill_statuses(ret, local_exists, remote_exists)

        self._log_missing_caches(ret)

        return ret

    def _fill_statuses(self, checksum_info_dir, local_exists, remote_exists):
        # Using sets because they are way faster for lookups
        local = set(local_exists)
        remote = set(remote_exists)

        for md5, info in checksum_info_dir.items():
            status = STATUS_MAP[(md5 in local, md5 in remote)]
            info["status"] = status

    def _get_chunks(self, download, remote, status_info, status, jobs):
        title = "Analysing status."

        progress.set_n_total(1)
        total = len(status_info)
        current = 0

        cache = []
        path_infos = []
        names = []
        for md5, info in status_info.items():
            if info["status"] == status:
                cache.append(self.checksum_to_path_info(md5))
                path_infos.append(remote.checksum_to_path_info(md5))
                names.append(info["name"])
            current += 1
            progress.update_target(title, current, total)

        progress.finish_target(title)

        progress.set_n_total(len(names))

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return list(
            zip(
                to_chunks(from_infos, jobs),
                to_chunks(to_infos, jobs),
                to_chunks(names, jobs),
            )
        )

    def _process(
        self,
        checksum_infos,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        msg = "Preparing to {} data {} '{}'"
        logger.info(
            msg.format(
                "download" if download else "upload",
                "from" if download else "to",
                remote.url,
            )
        )

        if download:
            func = remote.download
            status = STATUS_DELETED
        else:
            func = remote.upload
            status = STATUS_NEW

        if jobs is None:
            jobs = remote.JOBS

        status_info = self.status(
            checksum_infos,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        chunks = self._get_chunks(download, remote, status_info, status, jobs)

        if len(chunks) == 0:
            return 0

        futures = []
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            for from_infos, to_infos, names in chunks:
                res = executor.submit(func, from_infos, to_infos, names=names)
                futures.append(res)

        for f in futures:
            f.result()

        return len(chunks)

    def push(self, checksum_infos, remote, jobs=None, show_checksums=False):
        return self._process(
            checksum_infos,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=False,
        )

    def pull(self, checksum_infos, remote, jobs=None, show_checksums=False):
        return self._process(
            checksum_infos,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=True,
        )

    def _changed_cache_dir(self):
        mtime, size = get_mtime_and_size(self.cache_dir)
        inode = get_inode(self.cache_dir)

        existing_record = self.state.get_state_record_for_inode(inode)
        if existing_record:
            cached_mtime, cached_size, _, _ = existing_record
            changed = not (mtime == cached_mtime and size == cached_size)
        else:
            changed = True

        return changed

    def _log_missing_caches(self, checksum_info_dict):
        missing_caches = [
            (md5, info)
            for md5, info in checksum_info_dict.items()
            if info["status"] == STATUS_MISSING
        ]
        if missing_caches:
            missing_desc = "".join(
                [
                    "\nname: {}, md5: {}".format(info["name"], md5)
                    for md5, info in missing_caches
                ]
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
            tmp = os.path.join(os.path.dirname(path), "." + str(uuid.uuid4()))

            # The operations order is important here - if some application
            # would access the file during the process of copyfile then it
            # would get only the part of file. So, at first, the file should be
            # copied with the temporary name, and then original file should be
            # replaced by new.
            copyfile(
                path,
                tmp,
                name="Unprotecting '{}'".format(os.path.relpath(path)),
            )
            remove(path)
            os.rename(tmp, path)

        else:
            logger.debug(
                "Skipping copying for '{}', since it is not "
                "a symlink or a hardlink.".format(path)
            )

        os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)

    @staticmethod
    def _unprotect_dir(path):
        for path in walk_files(path):
            RemoteLOCAL._unprotect_file(path)

    @staticmethod
    def unprotect(path_info):
        path = path_info.path
        if not os.path.exists(path):
            raise DvcException(
                "can't unprotect non-existing data '{}'".format(path)
            )

        if os.path.isdir(path):
            RemoteLOCAL._unprotect_dir(path)
        else:
            RemoteLOCAL._unprotect_file(path)

    @staticmethod
    def protect(path_info):
        os.chmod(path_info.path, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)
