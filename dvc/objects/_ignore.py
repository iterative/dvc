from typing import TYPE_CHECKING, Iterator, Protocol

if TYPE_CHECKING:
    from .fs.base import AnyFSPath, FileSystem


class Ignore(Protocol):
    def find(
        self, fs: "FileSystem", path: "AnyFSPath"
    ) -> Iterator["AnyFSPath"]:
        ...
