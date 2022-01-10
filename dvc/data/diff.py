from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.file import HashFile

ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


@dataclass
class TreeEntry:
    in_cache: bool
    key: Tuple[str]
    oid: Optional["HashInfo"] = field(default=None)

    def __bool__(self):
        return bool(self.oid)

    def __eq__(self, other):
        if not isinstance(other, TreeEntry):
            return False

        if self.key != other.key:
            return False

        return self.oid == other.oid


@dataclass
class Change:
    old: TreeEntry
    new: TreeEntry

    @property
    def typ(self):
        if not self.old and not self.new:
            return UNCHANGED

        if self.old and not self.new:
            return DELETE

        if not self.old and self.new:
            return ADD

        if self.old != self.new:
            return MODIFY

        return UNCHANGED

    def __bool__(self):
        return self.typ != UNCHANGED


@dataclass
class DiffResult:
    added: List[Change] = field(default_factory=list, compare=True)
    modified: List[Change] = field(default_factory=list, compare=True)
    deleted: List[Change] = field(default_factory=list, compare=True)
    unchanged: List[Change] = field(default_factory=list, compare=True)

    def __bool__(self):
        return bool(self.added or self.modified or self.deleted)


ROOT = ("",)


def diff(
    old: Optional["HashFile"], new: Optional["HashFile"], cache
) -> DiffResult:
    from .tree import Tree

    if old is None and new is None:
        return DiffResult()

    def _get_keys(obj):
        if not obj:
            return []
        return [ROOT] + (
            [key for key, _, _ in obj] if isinstance(obj, Tree) else []
        )

    old_keys = set(_get_keys(old))
    new_keys = set(_get_keys(new))

    def _get_oid(obj, key):
        if not obj or key == ROOT:
            return obj.hash_info if obj else None

        entry_obj = obj.get(cache, key)
        return entry_obj.hash_info if entry_obj else None

    def _in_cache(oid, cache):
        from dvc.objects.errors import ObjectFormatError

        if not oid:
            return False

        try:
            cache.check(oid)
            return True
        except (FileNotFoundError, ObjectFormatError):
            return False

    ret = DiffResult()
    for key in old_keys | new_keys:
        old_oid = _get_oid(old, key)
        new_oid = _get_oid(new, key)

        change = Change(
            old=TreeEntry(_in_cache(old_oid, cache), key, old_oid),
            new=TreeEntry(_in_cache(new_oid, cache), key, new_oid),
        )

        if change.typ == ADD:
            ret.added.append(change)
        elif change.typ == MODIFY:
            ret.modified.append(change)
        elif change.typ == DELETE:
            ret.deleted.append(change)
        else:
            assert change.typ == UNCHANGED
            if not change.new.in_cache and not (
                change.new.oid and change.new.oid.isdir
            ):
                ret.modified.append(change)
            else:
                ret.unchanged.append(change)
    return ret
