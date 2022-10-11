from typing import Optional

from dvc.scm import iter_revs


def brancher(  # noqa: E302
    self,
    revs=None,
    all_branches=False,
    all_tags=False,
    all_commits=False,
    all_experiments=False,
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
    if not any([revs, all_branches, all_tags, all_commits, all_experiments]):
        yield ""
        return

    from dvc.fs import LocalFileSystem

    repo_root_parts = ()
    if self.fs.path.isin(self.root_dir, self.scm.root_dir):
        repo_root_parts = self.fs.path.relparts(
            self.root_dir, self.scm.root_dir
        )

    cwd_parts = ()
    if self.fs.path.isin(self.fs.path.getcwd(), self.scm.root_dir):
        cwd_parts = self.fs.path.relparts(
            self.fs.path.getcwd(), self.scm.root_dir
        )

    saved_fs = self.fs
    saved_root = self.root_dir

    scm = self.scm

    self.fs = LocalFileSystem(url=self.root_dir)
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
        from dvc.fs import GitFileSystem

        for sha, names in found_revs.items():
            self.__dict__.pop("index", None)
            self.fs = GitFileSystem(scm=scm, rev=sha)
            self.root_dir = self.fs.path.join("/", *repo_root_parts)

            if cwd_parts:
                cwd = self.fs.path.join(  # type: ignore[unreachable]
                    "/", *cwd_parts
                )
                self.fs.path.chdir(cwd)

            # ignore revs that don't contain repo root
            # (i.e. revs from before a subdir=True repo was init'ed)
            if self.fs.exists(self.root_dir):
                yield sha if sha_only else ",".join(names)
    finally:
        self.fs = saved_fs
        self.root_dir = saved_root
