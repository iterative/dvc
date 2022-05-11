from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dvc.objects.db import ObjectDB
    from dvc.objects.file import HashFile


@dataclass
class Meta:
    PARAM_SIZE = "size"
    PARAM_NFILES = "nfiles"
    PARAM_ISEXEC = "isexec"

    size: Optional[int] = field(default=None)
    nfiles: Optional[int] = field(default=None)
    isexec: Optional[bool] = field(default=False)

    obj: Optional["HashFile"] = field(default=None)
    odb: Optional["ObjectDB"] = field(default=None)
    remote: Optional[str] = field(default=None)

    @classmethod
    def from_dict(cls, d):
        if not d:
            return cls()

        size = d.pop(cls.PARAM_SIZE, None)
        nfiles = d.pop(cls.PARAM_NFILES, None)
        isexec = d.pop(cls.PARAM_ISEXEC, False)

        return cls(size=size, nfiles=nfiles, isexec=isexec)

    def to_dict(self):
        ret = OrderedDict()

        if self.size is not None:
            ret[self.PARAM_SIZE] = self.size

        if self.nfiles is not None:
            ret[self.PARAM_NFILES] = self.nfiles

        if self.isexec:
            ret[self.PARAM_ISEXEC] = self.isexec

        return ret
