from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from .base import FileSystem


class AuthError(Exception):
    pass


class ConfigError(Exception):
    pass


class RemoteMissingDepsError(Exception):
    def __init__(
        self,
        fs: "FileSystem",
        protocol: str,
        url: str,
        missing: List[str] = None,
    ) -> None:
        self.protocol = protocol
        self.fs = fs
        self.url = url
        self.missing_deps = missing or []
        super().__init__(
            f"filesystem for '{protocol}': '{type(fs)}' missing dependencies"
        )
