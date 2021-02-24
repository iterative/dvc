import posixpath
from operator import itemgetter

from funcy import cached_property
from pygtrie import Trie

from .hash_info import HashInfo


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


class DirInfo:
    PARAM_RELPATH = "relpath"

    def __init__(self):
        self._dict = {}

    def as_dict(self):
        return self._dict.copy()

    def add(self, key, hash_info):
        self.__dict__.pop("trie", None)
        self._dict[key] = hash_info

    def get(self, key, default=None):
        return self._dict.get(key, default)

    @cached_property
    def trie(self):
        return Trie(self._dict)

    @property
    def size(self):
        try:
            return sum(hash_info.size for _, hash_info in self._dict.items())
        except TypeError:
            return None

    @property
    def nfiles(self):
        return len(self._dict)

    def items(self):
        for key, hash_info in self._dict.items():
            yield key, hash_info

    @classmethod
    def from_list(cls, lst):
        ret = DirInfo()
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            hash_info = HashInfo.from_dict(entry)
            ret.add(parts, hash_info)
        return ret

    def to_list(self):
        # Sorting the list by path to ensure reproducibility
        return sorted(
            (
                {
                    # NOTE: not using hash_info.to_dict() because we don't want
                    # size/nfiles fields at this point.
                    hash_info.name: hash_info.value,
                    self.PARAM_RELPATH: posixpath.sep.join(parts),
                }
                for parts, hash_info in self._dict.items()  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def merge(self, ancestor, their):
        merged_dict = _merge(
            ancestor.as_dict(), self.as_dict(), their.as_dict(),
        )

        merged = DirInfo()
        for key, hi in merged_dict.items():
            merged.add(key, hi)
        return merged
