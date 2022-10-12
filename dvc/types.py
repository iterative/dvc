from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    TypedDict,
    Union,
)

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]
AnyPath = Union[str, StrPath]

OptStr = Optional[str]
TargetType = Union[List[str], str]
DictStrAny = Dict[str, Any]
DictAny = Dict[Any, Any]

ResultDict = TypedDict(
    "ResultDict", {"data": Any, "error": Exception}, total=False
)
ErrorHandler = Callable[[Exception], ResultDict]
