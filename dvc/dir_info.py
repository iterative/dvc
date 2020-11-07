import posixpath
from operator import itemgetter

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
        self.trie = Trie()

    @property
    def size(self):
        try:
            return sum(
                hash_info.size
                for _, hash_info in self.trie.iteritems()  # noqa: B301
            )
        except TypeError:
            return None

    @property
    def nfiles(self):
        return len(self.trie)

    def items(self, path_info=None):
        for key, hash_info in self.trie.iteritems():  # noqa: B301
            path = posixpath.sep.join(key)
            if path_info is not None:
                path = path_info / path
            yield path, hash_info

    @classmethod
    def from_list(cls, lst):
        ret = DirInfo()
        for _entry in lst:
            entry = _entry.copy()
            relpath = entry.pop(cls.PARAM_RELPATH)
            parts = tuple(relpath.split(posixpath.sep))
            ret.trie[parts] = HashInfo.from_dict(entry)
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
                for parts, hash_info in self.trie.iteritems()  # noqa: B301
            ),
            key=itemgetter(self.PARAM_RELPATH),
        )

    def merge(self, ancestor, their):
        merged = DirInfo()
        merged.trie = _merge(ancestor.trie, self.trie, their.trie)
        return merged
