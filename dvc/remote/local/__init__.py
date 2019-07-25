from __future__ import unicode_literals

from copy import copy

from dvc.scheme import Schemes
from dvc.remote.local.slow_link_detection import slow_link_guard
from dvc.utils.compat import str, makedirs, fspath_py35

import os
import stat
from shortuuid import uuid
import shutil
import logging

from dvc.system import System
from dvc.remote.base import (
    RemoteBASE,
    STATUS_MAP,
    STATUS_NEW,
    STATUS_DELETED,
    STATUS_MISSING,
)
from dvc.utils import (
    remove,
    move,
    copyfile,
    tmp_fname,
    file_md5,
    walk_files,
    relpath,
    dvc_walk,
)
from dvc.config import Config
from dvc.exceptions import DvcException
from dvc.progress import progress
from concurrent.futures import ThreadPoolExecutor

from dvc.path_info import PathInfo

logger = logging.getLogger(__name__)


class RemoteLOCAL(RemoteBASE):
    scheme = Schemes.LOCAL
    path_cls = PathInfo
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"

    UNPACKED_DIR_SUFFIX = ".unpacked"

    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_TYPE_MAP = {
        "copy": shutil.copyfile,
        "symlink": System.symlink,
        "hardlink": System.hardlink,
        "reflink": System.reflink,
    }

    def __init__(self, repo, config):
        super(RemoteLOCAL, self).__init__(repo, config)
        self.protected = config.get(Config.SECTION_CACHE_PROTECTED, False)

        types = config.get(Config.SECTION_CACHE_TYPE, None)
        if types:
            if isinstance(types, str):
                types = [t.strip() for t in types.split(",")]
            self.cache_types = types
        else:
            self.cache_types = copy(self.DEFAULT_CACHE_TYPES)

        # A clunky way to detect cache dir
        storagepath = config.get(Config.SECTION_LOCAL_STORAGEPATH, None)
        cache_dir = config.get(Config.SECTION_REMOTE_URL, storagepath)

        if cache_dir is not None and not os.path.isabs(cache_dir):
            cwd = config[Config.PRIVATE_CWD]
            cache_dir = os.path.abspath(os.path.join(cwd, cache_dir))

        if cache_dir is not None and not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

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

        clist = []
        for entry in os.listdir(fspath_py35(self.path_info)):
            subdir = self.path_info / entry
            if not os.path.isdir(fspath_py35(subdir)):
                continue
            clist.extend(
                subdir / cache for cache in os.listdir(fspath_py35(subdir))
            )

        return clist

    def get(self, md5):
        if not md5:
            return None

        return self.checksum_to_path_info(md5).url

    def exists(self, path_info):
        assert path_info.scheme == "local"
        return os.path.lexists(fspath_py35(path_info))

    def makedirs(self, path_info):
        if not self.exists(path_info):
            os.makedirs(fspath_py35(path_info))

    def link(self, from_info, to_info, link_type=None):
        from_path = from_info.fspath
        to_path = to_info.fspath

        assert os.path.isfile(from_path)

        dname = os.path.dirname(to_path)
        if not os.path.exists(dname):
            os.makedirs(dname)

        # NOTE: just create an empty file for an empty cache
        if os.path.getsize(from_path) == 0:
            open(to_path, "w+").close()

            msg = "Created empty file: {} -> {}".format(from_path, to_path)
            logger.debug(msg)
            return

        if not link_type:
            link_types = self.cache_types
        else:
            link_types = [link_type]

        self._try_links(from_info, to_info, link_types)

    @classmethod
    def _get_link_method(cls, link_type):
        try:
            return cls.CACHE_TYPE_MAP[link_type]
        except KeyError:
            raise DvcException(
                "Cache type: '{}' not supported!".format(link_type)
            )

    def _link(self, from_info, to_info, link_method):
        if self.exists(to_info):
            raise DvcException("Link '{}' already exists!".format(to_info))
        else:
            link_method(from_info.fspath, to_info.fspath)

        if self.protected:
            self.protect(to_info)

        msg = "Created {}'{}': {} -> {}".format(
            "protected " if self.protected else "",
            self.cache_types[0],
            from_info,
            to_info,
        )
        logger.debug(msg)

    @slow_link_guard
    def _try_links(self, from_info, to_info, link_types):
        i = len(link_types)
        while i > 0:
            link_method = self._get_link_method(link_types[0])
            try:
                self._link(from_info, to_info, link_method)
                return

            except DvcException as exc:
                msg = "Cache type '{}' is not supported: {}"
                logger.debug(msg.format(link_types[0], str(exc)))
                del link_types[0]
                i -= 1

        raise DvcException("no possible cache types left to try out.")

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

    def isfile(self, path_info):
        return os.path.isfile(fspath_py35(path_info))

    def isdir(self, path_info):
        return os.path.isdir(fspath_py35(path_info))

    def walk(self, path_info):
        return dvc_walk(path_info, self.repo.dvcignore)

    def get_file_checksum(self, path_info):
        return file_md5(fspath_py35(path_info))[0]

    def remove(self, path_info):
        if path_info.scheme != "local":
            raise NotImplementedError

        if self.exists(path_info):
            remove(path_info.fspath)

    def move(self, from_info, to_info):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        inp = from_info.fspath
        outp = to_info.fspath

        # moving in two stages to make the whole operation atomic in
        # case inp and outp are in different filesystems and actual
        # physical copying of data is happening
        tmp = "{}.{}".format(outp, str(uuid()))
        move(inp, tmp)
        move(tmp, outp)

    def cache_exists(self, md5s, jobs=None):
        return [
            checksum
            for checksum in progress(md5s)
            if not self.changed_cache_file(checksum)
        ]

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        makedirs(fspath_py35(to_info.parent), exist_ok=True)

        tmp_file = tmp_fname(to_info)
        copyfile(
            from_file, tmp_file, name=name, no_progress_bar=no_progress_bar
        )
        os.rename(tmp_file, fspath_py35(to_info))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        copyfile(
            fspath_py35(from_info),
            to_file,
            no_progress_bar=no_progress_bar,
            name=name,
        )

    def _group(self, checksum_infos, show_checksums=False):
        by_md5 = {}

        for info in checksum_infos:
            md5 = info[self.PARAM_CHECKSUM]

            if show_checksums:
                by_md5[md5] = {"name": md5}
                continue

            name = info["name"]
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
        logger.info(
            "Preparing to collect status from {}".format(remote.path_info)
        )
        ret = self._group(checksum_infos, show_checksums=show_checksums) or {}
        md5s = list(ret)

        logger.info("Collecting information from local cache...")
        local_exists = self.cache_exists(md5s, jobs=jobs)

        # This is a performance optimization. We can safely assume that,
        # if the resources that we want to fetch are already cached,
        # there's no need to check the remote storage for the existance of
        # those files.
        if download and sorted(local_exists) == sorted(md5s):
            remote_exists = local_exists
        else:
            logger.info("Collecting information from remote cache...")
            remote_exists = list(remote.cache_exists(md5s, jobs=jobs))

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

    def _get_plans(self, download, remote, status_info, status):
        cache = []
        path_infos = []
        names = []
        for md5, info in progress(
            status_info.items(), name="Analysing status"
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
        checksum_infos,
        remote,
        jobs=None,
        show_checksums=False,
        download=False,
    ):
        logger.info(
            "Preparing to {} '{}'".format(
                "download data from" if download else "upload data to",
                remote.path_info,
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

        plans = self._get_plans(download, remote, status_info, status)

        if len(plans[0]) == 0:
            return 0

        if jobs > 1:
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                fails = sum(executor.map(func, *plans))
        else:
            fails = sum(map(func, *plans))

        if fails:
            msg = "{} files failed to {}"
            raise DvcException(
                msg.format(fails, "download" if download else "upload")
            )

        return len(plans[0])

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
        for path in walk_files(path, self.repo.dvcignore):
            RemoteLOCAL._unprotect_file(path)

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
        os.chmod(
            fspath_py35(path_info), stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH
        )

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

        for entry in progress(dir_info, name="Created unpacked dir"):
            entry_cache_info = self.checksum_to_path_info(
                entry[self.PARAM_CHECKSUM]
            )
            relative_path = entry[self.PARAM_RELPATH]
            self.link(
                entry_cache_info, unpacked_dir_info / relative_path, "hardlink"
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
