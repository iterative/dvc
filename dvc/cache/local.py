import errno
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, wraps

from funcy import cached_property, concat

from dvc.exceptions import DownloadError, UploadError
from dvc.path_info import PathInfo
from dvc.progress import Tqdm

from ..remote.base import index_locked
from ..tree.local import LocalTree
from .base import (
    STATUS_DELETED,
    STATUS_MAP,
    STATUS_MISSING,
    STATUS_NEW,
    CloudCache,
)

logger = logging.getLogger(__name__)


def _log_exceptions(func, operation):
    @wraps(func)
    def wrapper(from_info, to_info, *args, **kwargs):
        try:
            func(from_info, to_info, *args, **kwargs)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            # NOTE: this means we ran out of file descriptors and there is no
            # reason to try to proceed, as we will hit this error anyways.
            # pylint: disable=no-member
            if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                raise

            logger.exception(
                "failed to %s '%s' to '%s'", operation, from_info, to_info
            )
            return 1

    return wrapper


class LocalCache(CloudCache):
    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_MODE = LocalTree.CACHE_MODE

    def __init__(self, tree):
        super().__init__(tree)
        self.cache_dir = tree.config.get("url")

    @property
    def cache_dir(self):
        return self.tree.path_info.fspath if self.tree.path_info else None

    @cache_dir.setter
    def cache_dir(self, value):
        self.tree.path_info = PathInfo(value) if value else None

    @classmethod
    def supported(cls, config):  # pylint: disable=unused-argument
        return True

    @cached_property
    def cache_path(self):
        return os.path.abspath(self.cache_dir)

    def hash_to_path(self, hash_):
        # NOTE: `self.cache_path` is already normalized so we can simply use
        # `os.sep` instead of `os.path.join`. This results in this helper
        # being ~5.5 times faster.
        return f"{self.cache_path}{os.sep}{hash_[0:2]}{os.sep}{hash_[2:]}"

    def hashes_exist(
        self, hashes, jobs=None, name=None
    ):  # pylint: disable=unused-argument
        return [
            hash_
            for hash_ in Tqdm(
                hashes,
                unit="file",
                desc="Querying "
                + ("cache in " + name if name else "local cache"),
            )
            if not self.changed_cache_file(hash_)
        ]

    def already_cached(self, path_info):
        assert path_info.scheme in ["", "local"]

        typ, current_md5 = self.tree.get_hash(path_info)

        assert typ == "md5"

        if not current_md5:
            return False

        return not self.changed_cache(current_md5)

    def _verify_link(self, path_info, link_type):
        if link_type == "hardlink" and self.tree.getsize(path_info) == 0:
            return

        super()._verify_link(path_info, link_type)

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
        {dir_hash: set(file_hash, ...)} which can be used to map
        a .dir file to its file contents.
        """
        logger.debug(
            f"Preparing to collect status from {remote.tree.path_info}"
        )
        md5s = set(named_cache.scheme_keys(self.tree.scheme))

        logger.debug("Collecting information from local cache...")
        local_exists = frozenset(
            self.hashes_exist(md5s, jobs=jobs, name=self.cache_dir)
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
            dir_md5s = set(named_cache.dir_keys(self.tree.scheme))
            if dir_md5s:
                remote_exists.update(
                    self._indexed_dir_hashes(named_cache, remote, dir_md5s)
                )
                md5s.difference_update(remote_exists)
            if md5s:
                remote_exists.update(
                    remote.hashes_exist(
                        md5s, jobs=jobs, name=str(remote.tree.path_info)
                    )
                )
        return self._make_status(
            named_cache, show_checksums, local_exists, remote_exists
        )

    def _make_status(
        self, named_cache, show_checksums, local_exists, remote_exists
    ):
        def make_names(hash_, names):
            return {"name": hash_ if show_checksums else " ".join(names)}

        dir_status = {}
        file_status = {}
        dir_contents = {}
        for hash_, item in named_cache[self.tree.scheme].items():
            if item.children:
                dir_status[hash_] = make_names(hash_, item.names)
                dir_contents[hash_] = set()
                for child_hash, child in item.children.items():
                    file_status[child_hash] = make_names(
                        child_hash, child.names
                    )
                    dir_contents[hash_].add(child_hash)
            else:
                file_status[hash_] = make_names(hash_, item.names)

        self._fill_statuses(dir_status, local_exists, remote_exists)
        self._fill_statuses(file_status, local_exists, remote_exists)

        self._log_missing_caches(dict(dir_status, **file_status))

        return dir_status, file_status, dir_contents

    def _indexed_dir_hashes(self, named_cache, remote, dir_md5s):
        # Validate our index by verifying all indexed .dir hashes
        # still exist on the remote
        indexed_dirs = set(remote.index.dir_hashes())
        indexed_dir_exists = set()
        if indexed_dirs:
            indexed_dir_exists.update(
                remote.tree.list_hashes_exists(indexed_dirs)
            )
            missing_dirs = indexed_dirs.difference(indexed_dir_exists)
            if missing_dirs:
                logger.debug(
                    "Remote cache missing indexed .dir hashes '{}', "
                    "clearing remote index".format(", ".join(missing_dirs))
                )
                remote.index.clear()

        # Check if non-indexed (new) dir hashes exist on remote
        dir_exists = dir_md5s.intersection(indexed_dir_exists)
        dir_exists.update(
            remote.tree.list_hashes_exists(dir_md5s - dir_exists)
        )

        # If .dir hash exists on the remote, assume directory contents
        # still exists on the remote
        for dir_hash in dir_exists:
            file_hashes = list(
                named_cache.child_keys(self.tree.scheme, dir_hash)
            )
            if dir_hash not in remote.index:
                logger.debug(
                    "Indexing new .dir '{}' with '{}' nested files".format(
                        dir_hash, len(file_hashes)
                    )
                )
                remote.index.update([dir_hash], file_hashes)
            yield dir_hash
            yield from file_hashes

    @staticmethod
    def _fill_statuses(hash_info_dir, local_exists, remote_exists):
        # Using sets because they are way faster for lookups
        local = set(local_exists)
        remote = set(remote_exists)

        for md5, info in hash_info_dir.items():
            status = STATUS_MAP[(md5 in local, md5 in remote)]
            info["status"] = status

    def _get_plans(self, download, remote, status_info, status):
        cache = []
        path_infos = []
        names = []
        hashes = []
        missing = []
        for md5, info in Tqdm(
            status_info.items(), desc="Analysing status", unit="file"
        ):
            if info["status"] == status:
                cache.append(self.tree.hash_to_path_info(md5))
                path_infos.append(remote.tree.hash_to_path_info(md5))
                names.append(info["name"])
                hashes.append(md5)
            elif info["status"] == STATUS_MISSING:
                missing.append(md5)

        if download:
            to_infos = cache
            from_infos = path_infos
        else:
            to_infos = path_infos
            from_infos = cache

        return (from_infos, to_infos, names, hashes), missing

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
                remote.tree.path_info,
            )
        )

        if download:
            func = partial(
                _log_exceptions(remote.tree.download, "download"),
                dir_mode=self.tree.dir_mode,
                file_mode=self.tree.file_mode,
            )
            status = STATUS_DELETED
            desc = "Downloading"
        else:
            func = _log_exceptions(remote.tree.upload, "upload")
            status = STATUS_NEW
            desc = "Uploading"

        if jobs is None:
            jobs = remote.tree.JOBS

        dir_status, file_status, dir_contents = self._status(
            named_cache,
            remote,
            jobs=jobs,
            show_checksums=show_checksums,
            download=download,
        )

        dir_plans, _ = self._get_plans(download, remote, dir_status, status)
        file_plans, missing_files = self._get_plans(
            download, remote, file_status, status
        )

        total = len(dir_plans[0]) + len(file_plans[0])
        if total == 0:
            return 0

        with Tqdm(total=total, unit="file", desc=desc) as pbar:
            func = pbar.wrap_fn(func)
            with ThreadPoolExecutor(max_workers=jobs) as executor:
                if download:
                    from_infos, to_infos, names, _ = (
                        d + f for d, f in zip(dir_plans, file_plans)
                    )
                    fails = sum(
                        executor.map(func, from_infos, to_infos, names)
                    )
                else:
                    # for uploads, push files first, and any .dir files last

                    file_futures = {}
                    for from_info, to_info, name, hash_ in zip(*file_plans):
                        file_futures[hash_] = executor.submit(
                            func, from_info, to_info, name
                        )
                    dir_futures = {}
                    for from_info, to_info, name, dir_hash in zip(*dir_plans):
                        # if for some reason a file contained in this dir is
                        # missing both locally and in the remote, we want to
                        # push whatever file content we have, but should not
                        # push .dir file
                        for file_hash in missing_files:
                            if file_hash in dir_contents[dir_hash]:
                                logger.debug(
                                    "directory '%s' contains missing files,"
                                    "skipping .dir file upload",
                                    name,
                                )
                                break
                        else:
                            wait_futures = {
                                future
                                for file_hash, future in file_futures.items()
                                if file_hash in dir_contents[dir_hash]
                            }
                            dir_futures[dir_hash] = executor.submit(
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
            for dir_hash, future in dir_futures.items():
                if future.result() == 0:
                    file_hashes = dir_contents[dir_hash]
                    logger.debug(
                        "Indexing pushed dir '{}' with "
                        "'{}' nested files".format(dir_hash, len(file_hashes))
                    )
                    remote.index.update([dir_hash], file_hashes)

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
                logger.error(f"failed to upload '{from_info}' to '{to_info}'")
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
    def _log_missing_caches(hash_info_dict):
        missing_caches = [
            (md5, info)
            for md5, info in hash_info_dict.items()
            if info["status"] == STATUS_MISSING
        ]
        if missing_caches:
            missing_desc = "\n".join(
                "name: {}, md5: {}".format(info["name"], md5)
                for md5, info in missing_caches
            )
            msg = (
                "Some of the cache files do not exist neither locally "
                "nor on remote. Missing cache files:\n{}".format(missing_desc)
            )
            logger.warning(msg)
