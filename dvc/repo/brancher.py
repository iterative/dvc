from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Optional

from scmrepo.git import Git

from dvc.exceptions import NotDvcRepoError
from dvc.log import logger
from dvc.scm import iter_revs

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logger.getChild(__name__)


def brancher(
    self,
    revs=None,
    all_branches=False,
    all_tags=False,
    all_commits=False,
    all_experiments=False,
    workspace=True,
    commit_date: Optional[str] = None,
    sha_only=False,
    num=1,
):
    """Generator that iterates over specified revisions.

    Args:
        revs (list): a list of revisions to iterate over.
        all_branches (bool): iterate over all available branches.
        all_commits (bool): iterate over all commits.
        all_tags (bool): iterate over all available tags.
        workspace (bool): include workspace.
        commit_date (str): Keep experiments from the commits after(include)
                            a certain date. Date must match the extended
                            ISO 8601 format (YYYY-MM-DD).
        sha_only (bool): only return git SHA for a revision.

    Yields:
        str: the display name for the currently selected fs, it could be:
            - a git revision identifier
            - empty string it there is no branches to iterate over
            - "workspace" if there are uncommitted changes in the SCM repo
    """
    if not any(
        [
            revs,
            all_branches,
            all_tags,
            all_commits,
            all_experiments,
            commit_date,
        ]
    ):
        yield ""
        return

    from dvc.fs import LocalFileSystem

    repo_root_parts: tuple[str, ...] = ()
    if self.fs.isin(self.root_dir, self.scm.root_dir):
        repo_root_parts = self.fs.relparts(self.root_dir, self.scm.root_dir)

    cwd_parts: tuple[str, ...] = ()
    if self.fs.isin(self.fs.getcwd(), self.scm.root_dir):
        cwd_parts = self.fs.relparts(self.fs.getcwd(), self.scm.root_dir)

    saved_fs = self.fs
    saved_root = self.root_dir
    saved_dvc_dir = self.dvc_dir

    scm = self.scm

    logger.trace("switching fs to workspace")
    self.fs = LocalFileSystem(url=self.root_dir)
    if workspace:
        yield "workspace"

    revs = revs.copy() if revs else []
    if "workspace" in revs:
        revs.remove("workspace")

    found_revs = iter_revs(
        scm,
        revs,
        all_branches=all_branches,
        all_tags=all_tags,
        all_commits=all_commits,
        all_experiments=all_experiments,
        commit_date=commit_date,
        num=num,
    )

    try:
        for sha, names in found_revs.items():
            try:
                _switch_fs(self, sha, repo_root_parts, cwd_parts)
                yield sha if sha_only else ",".join(names)
            except NotDvcRepoError:
                # ignore revs that don't contain repo root
                # (i.e. revs from before a subdir=True repo was init'ed)
                pass
    finally:
        self.fs = saved_fs
        self.root_dir = saved_root
        self.dvc_dir = saved_dvc_dir
        self._reset()


def _switch_fs(
    repo: "Repo",
    rev: str,
    repo_root_parts: tuple[str, ...],
    cwd_parts: tuple[str, ...],
):
    from dvc.fs import GitFileSystem, LocalFileSystem

    if rev == "workspace":
        logger.trace("switching fs to workspace")
        repo.fs = LocalFileSystem(url=repo.root_dir)
        return

    logger.trace("switching fs to revision %s", rev[:7])
    assert isinstance(repo.scm, Git)
    fs = GitFileSystem(scm=repo.scm, rev=rev)
    root_dir = repo.fs.join("/", *repo_root_parts)
    if not fs.exists(root_dir):
        raise NotDvcRepoError(f"Commit '{rev[:7]}' does not contain a DVC repo")

    repo.fs = fs
    repo.root_dir = root_dir
    repo.dvc_dir = fs.join(root_dir, repo.DVC_DIR)
    repo._reset()

    if cwd_parts:
        cwd = repo.fs.join("/", *cwd_parts)
        repo.fs.chdir(cwd)


@contextmanager
def switch(repo: "Repo", rev: str) -> Iterator[str]:
    """Switch to a specific revision."""
    from dvc.scm import resolve_rev

    if rev != "workspace":
        rev = resolve_rev(repo.scm, rev)

    repo_root_parts: tuple[str, ...] = ()
    if repo.fs.isin(repo.root_dir, repo.scm.root_dir):
        repo_root_parts = repo.fs.relparts(repo.root_dir, repo.scm.root_dir)

    cwd_parts: tuple[str, ...] = ()
    if repo.fs.isin(repo.fs.getcwd(), repo.scm.root_dir):
        cwd_parts = repo.fs.relparts(repo.fs.getcwd(), repo.scm.root_dir)

    saved_fs = repo.fs
    saved_root = repo.root_dir
    saved_dvc_dir = repo.dvc_dir
    try:
        _switch_fs(repo, rev, repo_root_parts, cwd_parts)
        yield rev
    finally:
        repo.fs = saved_fs
        repo.root_dir = saved_root
        repo.dvc_dir = saved_dvc_dir
        repo._reset()
