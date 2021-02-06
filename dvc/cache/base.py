import errno
import itertools
import json
import logging
import os
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from typing import Optional

from funcy import decorator
from shortuuid import uuid

from dvc.dir_info import DirInfo
from dvc.exceptions import CacheLinkError, DvcException
from dvc.progress import Tqdm
from dvc.remote.slow_link_detection import (  # type: ignore[attr-defined]
    slow_link_guard,
)

from ..tree.base import RemoteActionNotImplemented

logger = logging.getLogger(__name__)


class DirCacheError(DvcException):
    def __init__(self, hash_):
        super().__init__(
            f"Failed to load dir cache for hash value: '{hash_}'."
        )


@decorator
def use_state(call):
    tree = call._args[0].tree  # pylint: disable=protected-access
    with tree.state:
        return call()


class CloudCache:

    DEFAULT_VERIFY = False
    DEFAULT_CACHE_TYPES = ["copy"]
    CACHE_MODE: Optional[int] = None

    def __init__(self, tree):
        self.tree = tree
        self.repo = tree.repo

        self.verify = tree.config.get("verify", self.DEFAULT_VERIFY)
        self.cache_types = tree.config.get("type") or copy(
            self.DEFAULT_CACHE_TYPES
        )
        self.cache_type_confirmed = False
        self._dir_info = {}

    def get_dir_cache(self, hash_info):
        assert hash_info

        dir_info = self._dir_info.get(hash_info.value)
        if dir_info:
            return dir_info

        try:
            dir_info = self.load_dir_cache(hash_info)
        except DirCacheError:
            dir_info = DirInfo()

        self._dir_info[hash_info.value] = dir_info
        return dir_info

    def load_dir_cache(self, hash_info):
        path_info = self.tree.hash_to_path_info(hash_info.value)

        try:
            with self.tree.open(path_info, "r") as fobj:
                d = json.load(fobj)
        except (ValueError, FileNotFoundError) as exc:
            raise DirCacheError(hash_info) from exc

        if not isinstance(d, list):
            logger.error(
                "dir cache file format error '%s' [skipping the file]",
                path_info,
            )
            d = []

        return DirInfo.from_list(d)

    def link(self, from_info, to_info):
        self._link(from_info, to_info, self.cache_types)

    def move(self, from_info, to_info):
        self.tree.move(from_info, to_info)

    def makedirs(self, path_info):
        self.tree.makedirs(path_info)

    def _link(self, from_info, to_info, link_types):
        assert self.tree.isfile(from_info)

        self.makedirs(to_info.parent)

        self._try_links(from_info, to_info, link_types)

    def _verify_link(self, path_info, link_type):
        if self.cache_type_confirmed:
            return

        is_link = getattr(self.tree, f"is_{link_type}", None)
        if is_link and not is_link(path_info):
            self.tree.remove(path_info)
            raise DvcException(f"failed to verify {link_type}")

        self.cache_type_confirmed = True

    @slow_link_guard
    def _try_links(self, from_info, to_info, link_types):
        while link_types:
            link_method = getattr(self.tree, link_types[0])
            try:
                self._do_link(from_info, to_info, link_method)
                self._verify_link(to_info, link_types[0])
                return

            except DvcException as exc:
                logger.debug(
                    "Cache type '%s' is not supported: %s", link_types[0], exc
                )
                del link_types[0]

        raise CacheLinkError([to_info])

    def _do_link(self, from_info, to_info, link_method):
        if self.tree.exists(to_info):
            raise DvcException(f"Link '{to_info}' already exists!")

        link_method(from_info, to_info)

        logger.debug(
            "Created '%s': %s -> %s", self.cache_types[0], from_info, to_info,
        )

    def add(self, path_info, tree, hash_info, **kwargs):
        if not self.changed_cache_file(hash_info):
            return

        cache_info = self.tree.hash_to_path_info(hash_info.value)
        # using our makedirs to create dirs with proper permissions
        self.makedirs(cache_info.parent)
        if isinstance(tree, type(self.tree)):
            self.tree.move(path_info, cache_info)
        else:
            with tree.open(path_info, mode="rb") as fobj:
                self.tree.upload_fobj(fobj, cache_info)
            tree.state.save(path_info, hash_info)
        self.protect(cache_info)
        self.tree.state.save(cache_info, hash_info)

        callback = kwargs.get("download_callback")
        if callback:
            callback(1)

    def _save_file(self, path_info, tree, hash_info, **kwargs):
        assert hash_info

        self.add(path_info, tree, hash_info, **kwargs)

    def _transfer_file(self, from_tree, from_info):
        from dvc.utils import tmp_fname
        from dvc.utils.stream import HashedStreamReader

        tmp_info = self.tree.path_info / tmp_fname()
        with from_tree.open(
            from_info, mode="rb", chunk_size=from_tree.CHUNK_SIZE
        ) as stream:
            stream_reader = HashedStreamReader(stream)
            # Since we don't know the hash beforehand, we'll
            # upload it to a temporary location and then move
            # it.
            self.tree.upload_fobj(
                stream_reader,
                tmp_info,
                total=from_tree.getsize(from_info),
                desc=from_info.name,
            )

        hash_info = stream_reader.hash_info
        return tmp_info, hash_info

    def _transfer_directory_contents(self, from_tree, from_info, jobs, pbar):
        rel_path_infos = {}
        from_infos = from_tree.walk_files(from_info)

        def create_tasks(executor, amount):
            for entry_info in itertools.islice(from_infos, amount):
                pbar.total += 1
                task = executor.submit(
                    pbar.wrap_fn(self._transfer_file), from_tree, entry_info
                )
                rel_path_infos[task] = entry_info.relative_to(from_info)
                yield task

        pbar.total = 0
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            tasks = set(create_tasks(executor, jobs * 5))

            while tasks:
                done, tasks = futures.wait(
                    tasks, return_when=futures.FIRST_COMPLETED
                )
                tasks.update(create_tasks(executor, len(done)))
                for task in done:
                    yield rel_path_infos.pop(task), task.result()

    def _transfer_directory(
        self, from_tree, from_info, jobs, no_progress_bar=False
    ):
        dir_info = DirInfo()

        with Tqdm(total=1, unit="Files", disable=no_progress_bar) as pbar:
            for (
                entry_info,
                (entry_tmp_info, entry_hash),
            ) in self._transfer_directory_contents(
                from_tree, from_info, jobs, pbar
            ):
                self.add(entry_tmp_info, self.tree, entry_hash)
                dir_info.trie[entry_info.parts] = entry_hash

        local_cache = self.repo.cache.local
        (
            hash_info,
            to_info,
        ) = local_cache.get_dir_info_hash(  # pylint: disable=protected-access
            dir_info
        )

        self.add(to_info, self.repo.cache.local.tree, hash_info)
        return hash_info

    def transfer(self, from_tree, from_info, jobs=None, no_progress_bar=False):
        jobs = jobs or min((from_tree.jobs, self.tree.jobs))

        if from_tree.isdir(from_info):
            return self._transfer_directory(
                from_tree,
                from_info,
                jobs=jobs,
                no_progress_bar=no_progress_bar,
            )
        tmp_info, hash_info = self._transfer_file(from_tree, from_info)
        self.add(tmp_info, self.tree, hash_info)
        return hash_info

    def cache_is_copy(self, path_info):
        """Checks whether cache uses copies."""
        if self.cache_type_confirmed:
            return self.cache_types[0] == "copy"

        if set(self.cache_types) <= {"copy"}:
            return True

        workspace_file = path_info.with_name("." + uuid())
        test_cache_file = self.tree.path_info / ".cache_type_test_file"
        if not self.tree.exists(test_cache_file):
            with self.tree.open(test_cache_file, "wb") as fobj:
                fobj.write(bytes(1))
        try:
            self.link(test_cache_file, workspace_file)
        finally:
            self.tree.remove(workspace_file)
            self.tree.remove(test_cache_file)

        self.cache_type_confirmed = True
        return self.cache_types[0] == "copy"

    def get_dir_info_hash(self, dir_info):
        import tempfile

        from dvc.path_info import PathInfo
        from dvc.utils import tmp_fname

        tmp = tempfile.NamedTemporaryFile(delete=False).name
        with open(tmp, "w+") as fobj:
            json.dump(dir_info.to_list(), fobj, sort_keys=True)

        from_info = PathInfo(tmp)
        to_info = self.tree.path_info / tmp_fname("")
        self.tree.upload(from_info, to_info, no_progress_bar=True)

        hash_info = self.tree.get_file_hash(to_info)
        hash_info.value += self.tree.CHECKSUM_DIR_SUFFIX
        hash_info.dir_info = dir_info
        hash_info.nfiles = dir_info.nfiles

        return hash_info, to_info

    @use_state
    def save_dir_info(self, dir_info, hash_info=None):
        if (
            hash_info
            and hash_info.name == self.tree.PARAM_CHECKSUM
            and not self.changed_cache_file(hash_info)
        ):
            return hash_info

        hi, tmp_info = self.get_dir_info_hash(dir_info)
        self.add(tmp_info, self.tree, hi)

        return hi

    def _save_dir(
        self, path_info, tree, hash_info, filter_info=None, **kwargs,
    ):
        if not hash_info.dir_info:
            hash_info.dir_info = tree.cache.get_dir_cache(hash_info)
        hi = self.save_dir_info(hash_info.dir_info, hash_info)
        for entry_info, entry_hash in Tqdm(
            hi.dir_info.items(path_info),
            desc="Saving " + path_info.name,
            unit="file",
        ):
            if filter_info and not entry_info.isin_or_eq(filter_info):
                continue

            self._save_file(entry_info, tree, entry_hash, **kwargs)

        cache_info = self.tree.hash_to_path_info(hi.value)
        self.tree.state.save(cache_info, hi)
        tree.state.save(path_info, hi)

    @use_state
    def save(self, path_info, tree, hash_info, **kwargs):
        if path_info.scheme != self.tree.scheme:
            raise RemoteActionNotImplemented(
                f"save {path_info.scheme} -> {self.tree.scheme}",
                self.tree.scheme,
            )

        if not hash_info:
            kw = kwargs.copy()
            kw.pop("download_callback", None)
            hash_info = tree.get_hash(path_info, **kw)
            if not hash_info:
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), path_info
                )

        self._save(path_info, tree, hash_info, **kwargs)
        return hash_info

    def _save(self, path_info, tree, hash_info, **kwargs):
        to_info = self.tree.hash_to_path_info(hash_info.value)
        logger.debug("Saving '%s' to '%s'.", path_info, to_info)

        if tree.isdir(path_info):
            self._save_dir(path_info, tree, hash_info, **kwargs)
        else:
            self._save_file(path_info, tree, hash_info, **kwargs)

    # Override to return path as a string instead of PathInfo for clouds
    # which support string paths (see local)
    def hash_to_path(self, hash_):
        return self.tree.hash_to_path_info(hash_)

    def protect(self, path_info):  # pylint: disable=unused-argument
        pass

    def is_protected(self, path_info):  # pylint: disable=unused-argument
        return False

    def unprotect(self, path_info):  # pylint: disable=unused-argument
        pass

    def set_exec(self, path_info):  # pylint: disable=unused-argument
        pass

    def changed_cache_file(self, hash_info):
        """Compare the given hash with the (corresponding) actual one.

        - Use `State` as a cache for computed hashes
            + The entries are invalidated by taking into account the following:
                * mtime
                * inode
                * size
                * hash

        - Remove the file from cache if it doesn't match the actual hash
        """
        # Prefer string path over PathInfo when possible due to performance
        cache_info = self.hash_to_path(hash_info.value)
        if self.is_protected(cache_info):
            logger.trace(
                "Assuming '%s' is unchanged since it is read-only", cache_info
            )
            return False

        actual = self.tree.get_hash(cache_info)

        logger.trace(
            "cache '%s' expected '%s' actual '%s'",
            cache_info,
            hash_info,
            actual,
        )

        if not hash_info or not actual:
            return True

        if actual.value.split(".")[0] == hash_info.value.split(".")[0]:
            # making cache file read-only so we don't need to check it
            # next time
            self.protect(cache_info)
            return False

        if self.tree.exists(cache_info):
            logger.warning("corrupted cache file '%s'.", cache_info)
            self.tree.remove(cache_info)

        return True

    def _changed_dir_cache(self, hash_info, path_info=None, filter_info=None):
        if self.changed_cache_file(hash_info):
            return True

        dir_info = self.get_dir_cache(hash_info)
        for entry_info, entry_hash in dir_info.items(path_info):
            if path_info and filter_info:
                if not entry_info.isin_or_eq(filter_info):
                    continue

            if self.changed_cache_file(entry_hash):
                return True

        return False

    def changed_cache(self, hash_info, path_info=None, filter_info=None):
        if hash_info.isdir:
            return self._changed_dir_cache(
                hash_info, path_info=path_info, filter_info=filter_info
            )
        return self.changed_cache_file(hash_info)

    def get_files_number(self, path_info, hash_info, filter_info):
        from funcy.py3 import ilen

        if not hash_info:
            return 0

        if not hash_info.isdir:
            return 1

        if not filter_info:
            return self.get_dir_cache(hash_info).nfiles

        return ilen(
            filter_info.isin_or_eq(path_info / relpath)
            for relpath, _ in self.get_dir_cache(hash_info).items()
        )

    def _get_dir_size(self, dir_info):
        try:
            return sum(
                self.tree.getsize(self.tree.hash_to_path_info(hi.value))
                for _, hi in dir_info.items()
            )
        except FileNotFoundError:
            return None

    def merge(self, ancestor_info, our_info, their_info):
        assert our_info
        assert their_info

        if ancestor_info:
            ancestor = self.get_dir_cache(ancestor_info)
        else:
            ancestor = DirInfo()

        our = self.get_dir_cache(our_info)
        their = self.get_dir_cache(their_info)

        merged = our.merge(ancestor, their)
        hash_info = self.save_dir_info(merged)
        hash_info.size = self._get_dir_size(merged)
        return hash_info

    @use_state
    def get_hash(self, tree, path_info):
        hash_info = tree.get_hash(path_info)
        if not hash_info.isdir:
            assert hash_info.name == self.tree.PARAM_CHECKSUM
            return hash_info

        hi = self.save_dir_info(hash_info.dir_info, hash_info)
        hi.size = hash_info.size
        return hi

    def set_dir_info(self, hash_info):
        assert hash_info.isdir

        hash_info.dir_info = self.get_dir_cache(hash_info)
        hash_info.nfiles = hash_info.dir_info.nfiles

    def _list_paths(self, prefix=None, progress_callback=None):
        if prefix:
            if len(prefix) > 2:
                path_info = self.tree.path_info / prefix[:2] / prefix[2:]
            else:
                path_info = self.tree.path_info / prefix[:2]
            prefix = True
        else:
            path_info = self.tree.path_info
            prefix = False
        if progress_callback:
            for file_info in self.tree.walk_files(path_info, prefix=prefix):
                progress_callback()
                yield file_info.path
        else:
            yield from self.tree.walk_files(path_info, prefix=prefix)

    def _path_to_hash(self, path):
        parts = self.tree.PATH_CLS(path).parts[-2:]

        if not (len(parts) == 2 and parts[0] and len(parts[0]) == 2):
            raise ValueError(f"Bad cache file path '{path}'")

        return "".join(parts)

    def list_hashes(self, prefix=None, progress_callback=None):
        """Iterate over hashes in this tree.

        If `prefix` is specified, only hashes which begin with `prefix`
        will be returned.
        """
        for path in self._list_paths(prefix, progress_callback):
            try:
                yield self._path_to_hash(path)
            except ValueError:
                logger.debug(
                    "'%s' doesn't look like a cache file, skipping", path
                )

    def _hashes_with_limit(self, limit, prefix=None, progress_callback=None):
        count = 0
        for hash_ in self.list_hashes(prefix, progress_callback):
            yield hash_
            count += 1
            if count > limit:
                logger.debug(
                    "`list_hashes()` returned max '{}' hashes, "
                    "skipping remaining results".format(limit)
                )
                return

    def _max_estimation_size(self, hashes):
        # Max remote size allowed for us to use traverse method
        return max(
            self.tree.TRAVERSE_THRESHOLD_SIZE,
            len(hashes)
            / self.tree.TRAVERSE_WEIGHT_MULTIPLIER
            * self.tree.LIST_OBJECT_PAGE_SIZE,
        )

    def _estimate_remote_size(self, hashes=None, name=None):
        """Estimate tree size based on number of entries beginning with
        "00..." prefix.
        """
        prefix = "0" * self.tree.TRAVERSE_PREFIX_LEN
        total_prefixes = pow(16, self.tree.TRAVERSE_PREFIX_LEN)
        if hashes:
            max_hashes = self._max_estimation_size(hashes)
        else:
            max_hashes = None

        with Tqdm(
            desc="Estimating size of "
            + (f"cache in '{name}'" if name else "remote cache"),
            unit="file",
        ) as pbar:

            def update(n=1):
                pbar.update(n * total_prefixes)

            if max_hashes:
                hashes = self._hashes_with_limit(
                    max_hashes / total_prefixes, prefix, update
                )
            else:
                hashes = self.list_hashes(prefix, update)

            remote_hashes = set(hashes)
            if remote_hashes:
                remote_size = total_prefixes * len(remote_hashes)
            else:
                remote_size = total_prefixes
            logger.debug(f"Estimated remote size: {remote_size} files")
        return remote_size, remote_hashes

    def list_hashes_traverse(
        self, remote_size, remote_hashes, jobs=None, name=None
    ):
        """Iterate over all hashes found in this tree.
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
        num_pages = remote_size / self.tree.LIST_OBJECT_PAGE_SIZE
        if num_pages < 256 / self.tree.JOBS:
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
            if self.tree.TRAVERSE_PREFIX_LEN > 2:
                traverse_prefixes += [
                    "{0:0{1}x}".format(i, self.tree.TRAVERSE_PREFIX_LEN)
                    for i in range(
                        1, pow(16, self.tree.TRAVERSE_PREFIX_LEN - 2)
                    )
                ]
        with Tqdm(
            desc="Querying "
            + (f"cache in '{name}'" if name else "remote cache"),
            total=remote_size,
            initial=initial,
            unit="file",
        ) as pbar:

            def list_with_update(prefix):
                return list(
                    self.list_hashes(
                        prefix=prefix, progress_callback=pbar.update
                    )
                )

            with ThreadPoolExecutor(
                max_workers=jobs or self.tree.JOBS
            ) as executor:
                in_remote = executor.map(list_with_update, traverse_prefixes,)
                yield from itertools.chain.from_iterable(in_remote)

    def all(self, jobs=None, name=None):
        """Iterate over all hashes in this tree.

        Hashes will be fetched in parallel threads according to prefix
        (except for small remotes) and a progress bar will be displayed.
        """
        logger.debug(
            "Fetching all hashes from '{}'".format(
                name if name else "remote cache"
            )
        )

        if not self.tree.CAN_TRAVERSE:
            return self.list_hashes()

        remote_size, remote_hashes = self._estimate_remote_size(name=name)
        return self.list_hashes_traverse(
            remote_size, remote_hashes, jobs, name
        )

    def _remove_unpacked_dir(self, hash_):
        pass

    def gc(self, used, jobs=None):
        removed = False
        # hashes must be sorted to ensure we always remove .dir files first
        for hash_ in sorted(
            self.all(jobs, str(self.tree.path_info)),
            key=self.tree.is_dir_hash,
            reverse=True,
        ):
            if hash_ in used:
                continue
            path_info = self.tree.hash_to_path_info(hash_)
            if self.tree.is_dir_hash(hash_):
                # backward compatibility
                # pylint: disable=protected-access
                self._remove_unpacked_dir(hash_)
            self.tree.remove(path_info)
            removed = True

        return removed

    def list_hashes_exists(self, hashes, jobs=None, name=None):
        """Return list of the specified hashes which exist in this tree.
        Hashes will be queried individually.
        """
        logger.debug(
            "Querying {} hashes via object_exists".format(len(hashes))
        )
        with Tqdm(
            desc="Querying "
            + ("cache in " + name if name else "remote cache"),
            total=len(hashes),
            unit="file",
        ) as pbar:

            def exists_with_progress(path_info):
                ret = self.tree.exists(path_info)
                pbar.update_msg(str(path_info))
                return ret

            with ThreadPoolExecutor(
                max_workers=jobs or self.tree.JOBS
            ) as executor:
                path_infos = map(self.tree.hash_to_path_info, hashes)
                in_remote = executor.map(exists_with_progress, path_infos)
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
        assert self.tree.TRAVERSE_PREFIX_LEN >= 2

        hashes = set(hashes)
        if len(hashes) == 1 or not self.tree.CAN_TRAVERSE:
            remote_hashes = self.list_hashes_exists(hashes, jobs, name)
            return remote_hashes

        # Max remote size allowed for us to use traverse method
        remote_size, remote_hashes = self._estimate_remote_size(hashes, name)

        traverse_pages = remote_size / self.tree.LIST_OBJECT_PAGE_SIZE
        # For sufficiently large remotes, traverse must be weighted to account
        # for performance overhead from large lists/sets.
        # From testing with S3, for remotes with 1M+ files, object_exists is
        # faster until len(hashes) is at least 10k~100k
        if remote_size > self.tree.TRAVERSE_THRESHOLD_SIZE:
            traverse_weight = (
                traverse_pages * self.tree.TRAVERSE_WEIGHT_MULTIPLIER
            )
        else:
            traverse_weight = traverse_pages
        if len(hashes) < traverse_weight:
            logger.debug(
                "Large remote ('{}' hashes < '{}' traverse weight), "
                "using object_exists for remaining hashes".format(
                    len(hashes), traverse_weight
                )
            )
            return list(hashes & remote_hashes) + self.list_hashes_exists(
                hashes - remote_hashes, jobs, name
            )

        logger.debug("Querying '{}' hashes via traverse".format(len(hashes)))
        remote_hashes = set(
            self.list_hashes_traverse(remote_size, remote_hashes, jobs, name)
        )
        return list(hashes & set(remote_hashes))
