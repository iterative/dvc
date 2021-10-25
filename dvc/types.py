from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]
AnyPath = Union[str, StrPath]

OptStr = Optional[str]
TargetType = Union[List[str], str]
DictStrAny = Dict[str, Any]
DictAny = Dict[Any, Any]
