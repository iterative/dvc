from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional

HASH_DIR_SUFFIX = ".dir"


@dataclass
class HashInfo:
    name: Optional[str]
    value: Optional[str]
    obj_name: Optional[str] = field(default=None, compare=False)

    def __bool__(self):
        return bool(self.value)

    def __str__(self):
        return f"{self.name}: {self.value}"

    def __hash__(self):
        return hash((self.name, self.value))

    @classmethod
    def from_dict(cls, d):
        if not d:
            return cls(None, None)

        ((name, value),) = d.items()
        return cls(name, value)

    def to_dict(self):
        ret = OrderedDict()
        if not self:
            return ret

        ret[self.name] = self.value
        return ret

    @property
    def isdir(self):
        if not self:
            return False
        return self.value.endswith(HASH_DIR_SUFFIX)
