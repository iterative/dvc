import hashlib
import logging
from functools import wraps

from .index import RemoteIndex, RemoteIndexNoop

logger = logging.getLogger(__name__)


def index_locked(f):
    @wraps(f)
    def wrapper(obj, named_cache, remote, *args, **kwargs):
        if hasattr(remote, "index"):
            with remote.index:
                return f(obj, named_cache, remote, *args, **kwargs)
        return f(obj, named_cache, remote, *args, **kwargs)

    return wrapper


class Remote:
    """Cloud remote class.

    Provides methods for indexing and garbage collecting trees which contain
    DVC remotes.
    """

    INDEX_CLS = RemoteIndex

    def __init__(self, tree):
        self.tree = tree
        self.repo = tree.repo

        config = tree.config
        url = config.get("url")
        if url:
            index_name = hashlib.sha256(url.encode("utf-8")).hexdigest()
            self.index = self.INDEX_CLS(
                self.repo, index_name, dir_suffix=self.tree.CHECKSUM_DIR_SUFFIX
            )
        else:
            self.index = RemoteIndexNoop()

    def __repr__(self):
        return "{class_name}: '{path_info}'".format(
            class_name=type(self).__name__,
            path_info=self.tree.path_info or "No path",
        )

    @property
    def cache(self):
        return getattr(self.repo.cache, self.tree.scheme)

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
        indexed_hashes = set(self.index.intersection(hashes))
        hashes -= indexed_hashes
        logger.debug("Matched '{}' indexed hashes".format(len(indexed_hashes)))
        if not hashes:
            return indexed_hashes

        if len(hashes) == 1 or not self.tree.CAN_TRAVERSE:
            remote_hashes = self.tree.list_hashes_exists(hashes, jobs, name)
            return list(indexed_hashes) + remote_hashes

        # Max remote size allowed for us to use traverse method
        remote_size, remote_hashes = self.tree.estimate_remote_size(
            hashes, name
        )

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
            return (
                list(indexed_hashes)
                + list(hashes & remote_hashes)
                + self.tree.list_hashes_exists(
                    hashes - remote_hashes, jobs, name
                )
            )

        logger.debug("Querying '{}' hashes via traverse".format(len(hashes)))
        remote_hashes = set(
            self.tree.list_hashes_traverse(
                remote_size, remote_hashes, jobs, name
            )
        )
        return list(indexed_hashes) + list(hashes & set(remote_hashes))

    @classmethod
    @index_locked
    def gc(cls, named_cache, remote, jobs=None):
        tree = remote.tree
        used = set(named_cache.scheme_keys("local"))

        if tree.scheme != "":
            used.update(named_cache.scheme_keys(tree.scheme))

        removed = False
        # hashes must be sorted to ensure we always remove .dir files first
        for hash_ in sorted(
            tree.all(jobs, str(tree.path_info)),
            key=tree.is_dir_hash,
            reverse=True,
        ):
            if hash_ in used:
                continue
            path_info = tree.hash_to_path_info(hash_)
            if tree.is_dir_hash(hash_):
                # backward compatibility
                # pylint: disable=protected-access
                tree._remove_unpacked_dir(hash_)
            tree.remove(path_info)
            removed = True

        if removed and hasattr(remote, "index"):
            remote.index.clear()
        return removed
