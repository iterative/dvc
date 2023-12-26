import functools
from typing import TYPE_CHECKING, Any, Optional

from . import FileSystem

if TYPE_CHECKING:
    from scmrepo.fs import GitFileSystem as FsspecGitFileSystem
    from scmrepo.git.objects import GitTrie

    from dvc.scm import Git


class GitFileSystem(FileSystem):
    """Proxies the repo file access methods to Git objects"""

    protocol = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(
        self,
        path: Optional[str] = None,
        rev: Optional[str] = None,
        scm: Optional["Git"] = None,
        trie: Optional["GitTrie"] = None,
        **kwargs: Any,
    ) -> None:
        from dvc.scm import resolve_rev

        super().__init__()
        self.fs_args.update(
            {
                "path": path,
                "rev": rev,
                "scm": scm,
                "trie": trie,
                "rev_resolver": resolve_rev,
                **kwargs,
            }
        )

    @functools.cached_property
    def fs(
        self,
    ) -> "FsspecGitFileSystem":
        from scmrepo.fs import GitFileSystem as FsspecGitFileSystem

        return FsspecGitFileSystem(**self.fs_args)

    def getcwd(self):
        return self.fs.getcwd()

    def chdir(self, path):
        self.fs.chdir(path)

    @property
    def rev(self) -> str:
        return self.fs.rev

    def ls(self, path, detail=True, **kwargs):
        return self.fs.ls(path, detail=detail, **kwargs) or []
