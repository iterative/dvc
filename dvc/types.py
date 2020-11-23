from typing import TYPE_CHECKING, Union

from dvc.path_info import PathInfo, URLInfo

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]
DvcPath = Union[PathInfo, URLInfo]
AnyPath = Union[str, DvcPath, StrPath]
