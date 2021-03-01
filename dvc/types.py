from typing import TYPE_CHECKING, List, Optional, TypeVar, Union

from dvc.path_info import PathInfo, URLInfo

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]
DvcPath = Union[PathInfo, URLInfo]
AnyPath = Union[str, DvcPath, StrPath]

OptStr = Optional[str]

_Element = TypeVar("_Element")
SingleOrListOf = Union[_Element, List[_Element]]

TargetType = SingleOrListOf[str]
