import logging
import shlex
from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Set

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.scm.base import Base


logger = logging.getLogger(__name__)


class SCMContext:
    def __init__(self, scm: "Base", config: Dict[str, Any] = None) -> None:
        from funcy import get_in

        self.scm: "Base" = scm
        self.autostage: bool = get_in(
            config or {}, ["core", "autostage"], default=False
        )
        self.ignored_paths: List[str] = []
        self.files_to_track: Set[str] = set()
        self.quiet: bool = False

    def track_file(self, path: str) -> None:
        """Track file to remind user to track new files or autostage later."""
        return self.files_to_track.add(path)

    def track_changed_files(self) -> None:
        """Stage files that have changed."""
        if not self.files_to_track:
            return

        logger.debug("Staging files: %s", self.files_to_track)
        return self.scm.add(self.files_to_track)

    def ignore(self, path: str) -> None:
        from dvc.scm import SCMError
        from dvc.scm.exceptions import FileNotInRepoError

        logger.debug("Adding '%s' to gitignore file.", path)
        try:
            gitignore_file = self.scm.ignore(path)
        except FileNotInRepoError as exc:
            raise SCMError(str(exc))

        if gitignore_file:
            self.track_file(gitignore_file)
            return self.ignored_paths.append(path)

    def ignore_remove(self, path: str) -> None:
        from dvc.scm import SCMError
        from dvc.scm.exceptions import FileNotInRepoError

        logger.debug("Removing '%s' from gitignore file.", path)
        try:
            gitignore_file = self.scm.ignore_remove(path)
        except FileNotInRepoError as exc:
            raise SCMError(str(exc))

        if gitignore_file:
            return self.track_file(gitignore_file)

    @contextmanager
    def __call__(
        self, autostage: bool = None, quiet: bool = None
    ) -> Iterator["SCMContext"]:
        try:
            yield self
        except Exception:
            for path in self.ignored_paths:
                self.ignore_remove(path)
            raise
        finally:
            self.ignored_paths = []

        if not self.files_to_track:
            return

        if autostage is None:
            autostage = self.autostage
        if quiet is None:
            quiet = self.quiet

        from dvc.scm import NoSCM

        if autostage:
            self.track_changed_files()
        elif not quiet and not isinstance(self.scm, NoSCM):
            files = " ".join(map(shlex.quote, self.files_to_track))

            logger.info(
                "\nTo track the changes with git, run:\n\n"
                f"\tgit add {files}"
            )
            logger.info(
                "\nTo enable auto staging, run:\n\n"
                "\tdvc config core.autostage true"
            )

        self.files_to_track = set()

    def __enter__(self) -> "SCMContext":
        self._cm = self()  # pylint: disable=attribute-defined-outside-init
        return self._cm.__enter__()  # pylint: disable=no-member

    def __exit__(self, *exc_args) -> None:
        assert self._cm
        self._cm.__exit__(*exc_args)  # pylint: disable=no-member


def scm_context(method, autostage: bool = None, quiet: bool = None):
    @wraps(method)
    def run(repo: "Repo", *args, **kw):
        with repo.scm_context(autostage=autostage, quiet=quiet):
            return method(repo, *args, **kw)

    return run
