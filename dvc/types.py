from typing import TYPE_CHECKING, Any, AnyStr, Dict, List, Union

if TYPE_CHECKING:
    from os import PathLike

StrPath = Union[str, "PathLike[str]"]
BytesPath = Union[bytes, "PathLike[bytes]"]
GenericPath = Union[AnyStr, "PathLike[AnyStr]"]
StrOrBytesPath = Union[str, bytes, "PathLike[str]", "PathLike[bytes]"]

TargetType = Union[List[str], str]
DictStrAny = Dict[str, Any]
