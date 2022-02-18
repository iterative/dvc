import os
import threading
from typing import TYPE_CHECKING, Any

from funcy import cached_property, wrap_prop

from .fsspec_wrapper import FSSpecWrapper

if TYPE_CHECKING:
    from scmrepo.fs import GitFileSystem as FsspecGitFileSystem
    from scmrepo.git import Git
    from scmrepo.git.objects import GitTrie


class GitFileSystem(FSSpecWrapper):  # pylint:disable=abstract-method
    """Proxies the repo file access methods to Git objects"""

    sep = os.sep
    scheme = "local"

    def __init__(
        self,
        path: str = None,
        rev: str = None,
        scm: "Git" = None,
        trie: "GitTrie" = None,
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

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self) -> "FsspecGitFileSystem":
        from scmrepo.fs import GitFileSystem as FsspecGitFileSystem

        return FsspecGitFileSystem(**self.fs_args)

    @property
    def rev(self) -> str:
        return self.fs.rev
