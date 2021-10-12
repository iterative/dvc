from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Meta:
    PARAM_SIZE = "size"
    PARAM_NFILES = "nfiles"

    size: Optional[int] = field(default=None)
    nfiles: Optional[int] = field(default=None)

    @classmethod
    def from_dict(cls, d):
        if not d:
            return cls()

        size = d.pop(cls.PARAM_SIZE, None)
        nfiles = d.pop(cls.PARAM_NFILES, None)
        return cls(size=size, nfiles=nfiles)

    def to_dict(self):
        ret = OrderedDict()

        if self.size is not None:
            ret[self.PARAM_SIZE] = self.size

        if self.nfiles is not None:
            ret[self.PARAM_NFILES] = self.nfiles

        return ret
