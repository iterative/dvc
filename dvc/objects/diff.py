from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Tuple

if TYPE_CHECKING:
    from .file import HashFile

ADD = "add"
MODIFY = "modify"
DELETE = "delete"
UNCHANGED = "unchanged"


@dataclass
class TreeEntry:
    in_cache: bool
    key: Tuple[str]
    obj: Optional["HashFile"] = field(default=None)

    def __bool__(self):
        return bool(self.obj)

    def __eq__(self, other):
        if not isinstance(other, TreeEntry):
            return False

        if self.key != other.key or bool(self.obj) != bool(other.obj):
            return False

        if not self.obj:
            return False

        return self.obj.hash_info == other.obj.hash_info


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
            [key for key, _ in obj] if isinstance(obj, Tree) else []
        )

    old_keys = set(_get_keys(old))
    new_keys = set(_get_keys(new))

    def _get_obj(obj, key):
        if not obj or key == ROOT:
            return obj

        return obj.get(key)

    def _in_cache(obj, cache):
        from . import check
        from .errors import ObjectFormatError

        if not obj:
            return False

        try:
            check(cache, obj)
            return True
        except (FileNotFoundError, ObjectFormatError):
            return False

    ret = DiffResult()
    for key in old_keys | new_keys:
        old_obj = _get_obj(old, key)
        new_obj = _get_obj(new, key)

        change = Change(
            old=TreeEntry(_in_cache(old_obj, cache), key, old_obj),
            new=TreeEntry(_in_cache(new_obj, cache), key, new_obj),
        )

        if change.typ == ADD:
            ret.added.append(change)
        elif change.typ == MODIFY:
            ret.modified.append(change)
        elif change.typ == DELETE:
            ret.deleted.append(change)
        else:
            assert change.typ == UNCHANGED
            if not change.new.in_cache and not isinstance(
                change.new.obj, Tree
            ):
                ret.modified.append(change)
            else:
                ret.unchanged.append(change)
    return ret
