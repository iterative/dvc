import itertools
import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from copy import copy
from typing import TYPE_CHECKING, Optional

from dvc.objects.errors import ObjectDBPermissionError, ObjectFormatError
from dvc.objects.file import HashFile
from dvc.progress import Tqdm

if TYPE_CHECKING:
    from typing import Tuple

    from dvc.fs.base import FileSystem
    from dvc.hash_info import HashInfo
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


class ObjectDB:

    DEFAULT_VERIFY = False
    DEFAULT_CACHE_TYPES = ["copy"]
    CACHE_MODE: Optional[int] = None

    def __init__(self, fs: "FileSystem", path: str, **config):
        from dvc.state import StateNoop

        self.fs = fs
        self.fs_path = path
        self.state = config.get("state", StateNoop())
        self.verify = config.get("verify", self.DEFAULT_VERIFY)
        self.cache_types = config.get("type") or copy(self.DEFAULT_CACHE_TYPES)
        self.slow_link_warning = config.get("slow_link_warning", True)
        self.tmp_dir = config.get("tmp_dir")
        self.read_only = config.get("read_only", False)

    @property
    def config(self):
        return {
            "state": self.state,
            "verify": self.verify,
            "type": self.cache_types,
            "slow_link_warning": self.slow_link_warning,
            "tmp_dir": self.tmp_dir,
            "read_only": self.read_only,
        }

    def __eq__(self, other):
        return (
            self.fs == other.fs
            and self.fs_path == other.fs_path
            and self.read_only == other.read_only
        )

    def __hash__(self):
        return hash((self.fs.scheme, self.fs_path))

    def exists(self, hash_info: "HashInfo"):
        return self.fs.exists(self.hash_to_path(hash_info.value))

    def move(self, from_info, to_info):
        self.fs.move(from_info, to_info)

    def makedirs(self, fs_path):
        self.fs.makedirs(fs_path)

    def get(self, hash_info: "HashInfo"):
        """get raw object"""
        return HashFile(
            self.hash_to_path(hash_info.value),
            self.fs,
            hash_info,
        )

    def _add_file(
        self,
        from_fs: "FileSystem",
        from_info: "AnyPath",
        to_info: "AnyPath",
        _hash_info: "HashInfo",
        hardlink: bool = False,
    ):
        from dvc import fs

        self.makedirs(self.fs.path.parent(to_info))
        return fs.utils.transfer(
            from_fs,
            from_info,
            self.fs,
            to_info,
            hardlink=hardlink,
        )

    def add(
        self,
        fs_path: "AnyPath",
        fs: "FileSystem",
        hash_info: "HashInfo",
        hardlink: bool = False,
        verify: Optional[bool] = None,
    ):
        if self.read_only:
            raise ObjectDBPermissionError("Cannot add to read-only ODB")

        if verify is None:
            verify = self.verify
        try:
            self.check(hash_info, check_hash=verify)
            return
        except (ObjectFormatError, FileNotFoundError):
            pass

        cache_fs_path = self.hash_to_path(hash_info.value)
        self._add_file(
            fs, fs_path, cache_fs_path, hash_info, hardlink=hardlink
        )

        try:
            if verify:
                self.check(hash_info, check_hash=True)
            self.protect(cache_fs_path)
            self.state.save(cache_fs_path, self.fs, hash_info)
        except (ObjectFormatError, FileNotFoundError):
            pass

    def hash_to_path(self, hash_):
        return self.fs.path.join(self.fs_path, hash_[0:2], hash_[2:])

    def protect(self, fs_path):  # pylint: disable=unused-argument
        pass

    def is_protected(self, fs_path):  # pylint: disable=unused-argument
        return False

    def unprotect(self, fs_path):  # pylint: disable=unused-argument
        pass

    def set_exec(self, fs_path):  # pylint: disable=unused-argument
        pass

    def check(
        self,
        hash_info: "HashInfo",
        check_hash: bool = True,
    ):
        """Compare the given hash with the (corresponding) actual one if
        check_hash is specified, or just verify the existence of the cache
        files on the filesystem.

        - Use `State` as a cache for computed hashes
            + The entries are invalidated by taking into account the following:
                * mtime
                * inode
                * size
                * hash

        - Remove the file from cache if it doesn't match the actual hash
        """

        obj = self.get(hash_info)
        if self.is_protected(obj.fs_path):
            logger.trace(  # type: ignore[attr-defined]
                "Assuming '%s' is unchanged since it is read-only",
                obj.fs_path,
            )
            return

        try:
            obj.check(self, check_hash=check_hash)
        except ObjectFormatError:
            logger.warning("corrupted cache file '%s'.", obj.fs_path)
            with suppress(FileNotFoundError):
                self.fs.remove(obj.fs_path)
            raise

        if check_hash:
            # making cache file read-only so we don't need to check it
            # next time
            self.protect(obj.fs_path)

    def _list_paths(self, prefix: str = None):
        prefix = prefix or ""
        parts: "Tuple[str, ...]" = (self.fs_path,)
        if prefix:
            parts = *parts, prefix[:2]
        if len(prefix) > 2:
            parts = *parts, prefix[2:]
        yield from self.fs.find(self.fs.path.join(*parts), prefix=bool(prefix))

    def _path_to_hash(self, path):
        parts = self.fs.path.parts(path)[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def _list_hashes(self, prefix=None):
        """Iterate over hashes in this fs.

        If `prefix` is specified, only hashes which begin with `prefix`
        will be returned.
        """
        for path in self._list_paths(prefix):
            try:
                yield self._path_to_hash(path)
            except ValueError:
                logger.debug(
                    "'%s' doesn't look like a cache file, skipping", path
                )

    def _hashes_with_limit(self, limit, prefix=None):
        count = 0
        for hash_ in self._list_hashes(prefix):
            yield hash_
            count += 1
            if count > limit:
                logger.debug(
                    "`_list_hashes()` returned max '{}' hashes, "
                    "skipping remaining results".format(limit)
                )
                return

    def _max_estimation_size(self, hashes):
        # Max remote size allowed for us to use traverse method
        return max(
            self.fs.TRAVERSE_THRESHOLD_SIZE,
            len(hashes)
            / self.fs.TRAVERSE_WEIGHT_MULTIPLIER
            * self.fs.LIST_OBJECT_PAGE_SIZE,
        )

    def _estimate_remote_size(self, hashes=None, name=None):
        """Estimate fs size based on number of entries beginning with
        "00..." prefix.
        """
        prefix = "0" * self.fs.TRAVERSE_PREFIX_LEN
        total_prefixes = pow(16, self.fs.TRAVERSE_PREFIX_LEN)
        if hashes:
            max_hashes = self._max_estimation_size(hashes)
        else:
            max_hashes = None

        with Tqdm(
            desc="Estimating size of "
            + (f"cache in '{name}'" if name else "remote cache"),
            unit="file",
        ) as pbar:

            def iter_with_pbar(hashes):
                for hash_ in hashes:
                    pbar.update(total_prefixes)
                    yield hash_

            if max_hashes:
                hashes = self._hashes_with_limit(
                    max_hashes / total_prefixes, prefix
                )
            else:
                hashes = self._list_hashes(prefix)

            remote_hashes = set(iter_with_pbar(hashes))
            if remote_hashes:
                remote_size = total_prefixes * len(remote_hashes)
            else:
                remote_size = total_prefixes
            logger.debug(f"Estimated remote size: {remote_size} files")
        return remote_size, remote_hashes

    def _list_hashes_traverse(
        self, remote_size, remote_hashes, jobs=None, name=None
    ):
        """Iterate over all hashes found in this fs.
        Hashes are fetched in parallel according to prefix, except in
        cases where the remote size is very small.

        All hashes from the remote (including any from the size
        estimation step passed via the `remote_hashes` argument) will be
        returned.

        NOTE: For large remotes the list of hashes will be very
        big(e.g. 100M entries, md5 for each is 32 bytes, so ~3200Mb list)
        and we don't really need all of it at the same time, so it makes
        sense to use a generator to gradually iterate over it, without
        keeping all of it in memory.
        """
        num_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        if num_pages < 256 / self.fs.jobs:
            # Fetching prefixes in parallel requires at least 255 more
            # requests, for small enough remotes it will be faster to fetch
            # entire cache without splitting it into prefixes.
            #
            # NOTE: this ends up re-fetching hashes that were already
            # fetched during remote size estimation
            traverse_prefixes = [None]
            initial = 0
        else:
            yield from remote_hashes
            initial = len(remote_hashes)
            traverse_prefixes = [f"{i:02x}" for i in range(1, 256)]
            if self.fs.TRAVERSE_PREFIX_LEN > 2:
                traverse_prefixes += [
                    "{0:0{1}x}".format(i, self.fs.TRAVERSE_PREFIX_LEN)
                    for i in range(1, pow(16, self.fs.TRAVERSE_PREFIX_LEN - 2))
                ]
        with Tqdm(
            desc="Querying "
            + (f"cache in '{name}'" if name else "remote cache"),
            total=remote_size,
            initial=initial,
            unit="file",
        ) as pbar:
            from funcy import collecting

            with ThreadPoolExecutor(
                max_workers=jobs or self.fs.jobs
            ) as executor:
                list_hashes = collecting(pbar.wrap_fn(self._list_hashes))
                in_remote = executor.map(list_hashes, traverse_prefixes)
                yield from itertools.chain.from_iterable(in_remote)

    def all(self, jobs=None, name=None):
        """Iterate over all hashes in this fs.

        Hashes will be fetched in parallel threads according to prefix
        (except for small remotes) and a progress bar will be displayed.
        """
        logger.debug(
            "Fetching all hashes from '{}'".format(
                name if name else "remote cache"
            )
        )

        if not self.fs.CAN_TRAVERSE:
            return self._list_hashes()

        remote_size, remote_hashes = self._estimate_remote_size(name=name)
        return self._list_hashes_traverse(
            remote_size, remote_hashes, jobs=jobs, name=name
        )

    def _remove_unpacked_dir(self, hash_):
        pass

    def list_hashes_exists(self, hashes, jobs=None, name=None):
        """Return list of the specified hashes which exist in this fs.
        Hashes will be queried individually.
        """
        logger.debug(f"Querying {len(hashes)} hashes via object_exists")
        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(hashes),
            unit="file",
        ) as pbar:

            def exists_with_progress(fs_path):
                ret = self.fs.exists(fs_path)
                pbar.update_msg(fs_path)
                return ret

            with ThreadPoolExecutor(
                max_workers=jobs or self.fs.jobs
            ) as executor:
                fs_paths = map(self.hash_to_path, hashes)
                in_remote = executor.map(exists_with_progress, fs_paths)
                ret = list(itertools.compress(hashes, in_remote))
                return ret

    def hashes_exist(self, hashes, jobs=None, name=None):
        """Check if the given hashes are stored in the remote.

        There are two ways of performing this check:

        - Traverse method: Get a list of all the files in the remote
            (traversing the cache directory) and compare it with
            the given hashes. Cache entries will be retrieved in parallel
            threads according to prefix (i.e. entries starting with, "00...",
            "01...", and so on) and a progress bar will be displayed.

        - Exists method: For each given hash, run the `exists`
            method and filter the hashes that aren't on the remote.
            This is done in parallel threads.
            It also shows a progress bar when performing the check.

        The reason for such an odd logic is that most of the remotes
        take much shorter time to just retrieve everything they have under
        a certain prefix (e.g. s3, gs, ssh, hdfs). Other remotes that can
        check if particular file exists much quicker, use their own
        implementation of hashes_exist (see ssh, local).

        Which method to use will be automatically determined after estimating
        the size of the remote cache, and comparing the estimated size with
        len(hashes). To estimate the size of the remote cache, we fetch
        a small subset of cache entries (i.e. entries starting with "00...").
        Based on the number of entries in that subset, the size of the full
        cache can be estimated, since the cache is evenly distributed according
        to hash.

        Returns:
            A list with hashes that were found in the remote
        """
        # Remotes which do not use traverse prefix should override
        # hashes_exist() (see ssh, local)
        assert self.fs.TRAVERSE_PREFIX_LEN >= 2

        # During the tests, for ensuring that the traverse behavior
        # is working we turn on this option. It will ensure the
        # _list_hashes_traverse() is called.
        always_traverse = getattr(self.fs, "_ALWAYS_TRAVERSE", False)

        hashes = set(hashes)
        if (
            len(hashes) == 1 or not self.fs.CAN_TRAVERSE
        ) and not always_traverse:
            remote_hashes = self.list_hashes_exists(hashes, jobs, name)
            return remote_hashes

        # Max remote size allowed for us to use traverse method
        remote_size, remote_hashes = self._estimate_remote_size(hashes, name)

        traverse_pages = remote_size / self.fs.LIST_OBJECT_PAGE_SIZE
        # For sufficiently large remotes, traverse must be weighted to account
        # for performance overhead from large lists/sets.
        # From testing with S3, for remotes with 1M+ files, object_exists is
        # faster until len(hashes) is at least 10k~100k
        if remote_size > self.fs.TRAVERSE_THRESHOLD_SIZE:
            traverse_weight = (
                traverse_pages * self.fs.TRAVERSE_WEIGHT_MULTIPLIER
            )
        else:
            traverse_weight = traverse_pages
        if len(hashes) < traverse_weight and not always_traverse:
            logger.debug(
                "Large remote ('{}' hashes < '{}' traverse weight), "
                "using object_exists for remaining hashes".format(
                    len(hashes), traverse_weight
                )
            )
            return list(hashes & remote_hashes) + self.list_hashes_exists(
                hashes - remote_hashes, jobs, name
            )

        logger.debug(f"Querying '{len(hashes)}' hashes via traverse")
        remote_hashes = set(
            self._list_hashes_traverse(
                remote_size, remote_hashes, jobs=jobs, name=name
            )
        )
        return list(hashes & set(remote_hashes))
