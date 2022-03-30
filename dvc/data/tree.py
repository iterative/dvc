import json
import logging
import posixpath
from typing import TYPE_CHECKING, Dict, Iterable, Optional, Tuple

from funcy import cached_property

from dvc.objects.errors import ObjectFormatError
from dvc.objects.file import HashFile

from .stage import get_file_hash

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB

    from .meta import Meta

logger = logging.getLogger(__name__)


class TreeError(Exception):
    pass


def _try_load(
    odbs: Iterable["ObjectDB"],
    hash_info: "HashInfo",
) -> Optional["HashFile"]:
    for odb in odbs:
        if not odb:
            continue

        try:
            return Tree.load(odb, hash_info)
        except (FileNotFoundError, ObjectFormatError):
            pass

    return None


class Tree(HashFile):
    PARAM_RELPATH = "relpath"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dict: Dict[Tuple[str], Tuple["Meta", "HashInfo"]] = {}

    @cached_property
    def _trie(self):
        from pygtrie import Trie

        return Trie(self._dict)

    def add(self, key: Tuple[str], meta: "Meta", oid: "HashInfo"):
        self.__dict__.pop("trie", None)
        self._dict[key] = (meta, oid)

    def digest(self, hash_info: Optional["HashInfo"] = None):
        from dvc.fs.memory import MemoryFileSystem
        from dvc.utils import tmp_fname

        memfs = MemoryFileSystem()
        fs_path = "memory://{}".format(tmp_fname(""))
        with memfs.open(fs_path, "wb") as fobj:
            fobj.write(self.as_bytes())
        self.fs = memfs
        self.fs_path = fs_path
        if hash_info:
            self.hash_info = hash_info
        else:
            _, self.hash_info = get_file_hash(fs_path, memfs, "md5")
            assert self.hash_info.value
            self.hash_info.value += ".dir"

    def _load(self, key, meta, hash_info):
        if hash_info and hash_info.isdir and not meta.obj:
            meta.obj = _try_load([meta.odb, meta.remote], hash_info)
            if meta.obj:
                for ikey, value in meta.obj.iteritems():
                    self._trie[key + ikey] = value
                    self._dict[key + ikey] = value

    def iteritems(self, prefix=None):
        kwargs = {}
        if prefix:
            kwargs = {"prefix": prefix}
            item = self._trie.longest_prefix(prefix)
            if item:
                key, (meta, hash_info) = item
                self._load(key, meta, hash_info)

        for key, (meta, hash_info) in self._trie.iteritems(**kwargs):
            self._load(key, meta, hash_info)
            yield key, (meta, hash_info)

    def shortest_prefix(self, *args, **kwargs):
        return self._trie.shortest_prefix(*args, **kwargs)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        yield from (
            (key, value[0], value[1]) for key, value in self._dict.items()
        )

    def as_dict(self):
        return self._dict.copy()

    def as_list(self):
        from operator import itemgetter

        # Sorting the list by path to ensure reproducibility
        return sorted(
            (
                {
                    # NOTE: not using hash_info.to_dict() because we don't want
                    # size/nfiles fields at this point.
                    oid.name: oid.value,
                    self.PARAM_RELPATH: posixpath.sep.join(parts),
                }
                for parts, _, oid in self  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def as_bytes(self):
        return json.dumps(self.as_list(), sort_keys=True).encode("utf-8")

    @classmethod
    def from_list(cls, lst):
        from dvc.hash_info import HashInfo

        tree = cls(None, None, None)
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            hash_info = HashInfo.from_dict(entry)
            tree.add(parts, None, hash_info)
        return tree

    @classmethod
    def load(cls, odb, hash_info):
        obj = odb.get(hash_info)

        try:
            with obj.fs.open(obj.fs_path, "r") as fobj:
                raw = json.load(fobj)
        except ValueError as exc:
            raise ObjectFormatError(f"{obj} is corrupted") from exc

        if not isinstance(raw, list):
            logger.error(
                "dir cache file format error '%s' [skipping the file]",
                obj.fs_path,
            )
            raise ObjectFormatError(f"{obj} is corrupted")

        tree = cls.from_list(raw)
        tree.fs_path = obj.fs_path
        tree.fs = obj.fs
        tree.hash_info = hash_info

        return tree

    def filter(self, prefix: Tuple[str]) -> Optional["Tree"]:
        """Return a filtered copy of this tree that only contains entries
        inside prefix.

        The returned tree will contain the original tree's hash_info and
        fs_path.

        Returns an empty tree if no object exists at the specified prefix.
        """
        tree = Tree(self.fs_path, self.fs, self.hash_info)
        try:
            for key, (meta, oid) in self._trie.items(prefix):
                tree.add(key, meta, oid)
        except KeyError:
            pass
        return tree

    def get(self, odb, prefix: Tuple[str]) -> Optional[HashFile]:
        """Return object at the specified prefix in this tree.

        Returns None if no object exists at the specified prefix.
        """
        _, oid = self._dict.get(prefix) or (None, None)
        if oid:
            return odb.get(oid)

        tree = Tree(None, None, None)
        depth = len(prefix)
        try:
            for key, (meta, entry_oid) in self._trie.items(prefix):
                tree.add(key[depth:], meta, entry_oid)
        except KeyError:
            return None
        tree.digest()
        return tree

    def ls(self, prefix=None):
        kwargs = {}
        if prefix:
            kwargs["prefix"] = prefix

        meta, hash_info = self._trie.get(prefix, (None, None))
        if hash_info and hash_info.isdir and meta and not meta.obj:
            raise TreeError

        ret = []

        def node_factory(_, key, children, *args):
            if key == prefix:
                list(children)
            else:
                ret.append(key[-1])

        self._trie.traverse(node_factory, **kwargs)

        return ret


def du(odb, tree):
    try:
        return sum(
            odb.fs.getsize(odb.hash_to_path(oid.value)) for _, _, oid in tree
        )
    except FileNotFoundError:
        return None


def _diff(ancestor, other, allow_removed=False):
    from dictdiffer import diff

    from dvc.exceptions import MergeError

    allowed = ["add"]
    if allow_removed:
        allowed.append("remove")

    result = list(diff(ancestor, other))
    for typ, _, _ in result:
        if typ not in allowed:
            raise MergeError(
                "unable to auto-merge directories with diff that contains "
                f"'{typ}'ed files"
            )
    return result


def _merge(ancestor, our, their):
    import copy

    from dictdiffer import patch

    our_diff = _diff(ancestor, our)
    if not our_diff:
        return copy.deepcopy(their)

    their_diff = _diff(ancestor, their)
    if not their_diff:
        return copy.deepcopy(our)

    # make sure there are no conflicting files
    _diff(our, their, allow_removed=True)

    return patch(our_diff + their_diff, ancestor)


def merge(odb, ancestor_info, our_info, their_info):
    from . import load

    assert our_info
    assert their_info

    if ancestor_info:
        ancestor = load(odb, ancestor_info)
    else:
        ancestor = Tree(None, None, None)

    our = load(odb, our_info)
    their = load(odb, their_info)

    merged_dict = _merge(ancestor.as_dict(), our.as_dict(), their.as_dict())

    merged = Tree(None, None, None)
    for key, (meta, oid) in merged_dict.items():
        merged.add(key, meta, oid)
    merged.digest()

    return merged
