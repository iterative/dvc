from dvc.scm import iter_revs


def brancher(  # noqa: E302
    self,
    revs=None,
    all_branches=False,
    all_tags=False,
    all_commits=False,
    all_experiments=False,
    sha_only=False,
):
    """Generator that iterates over specified revisions.

    Args:
        revs (list): a list of revisions to iterate over.
        all_branches (bool): iterate over all available branches.
        all_commits (bool): iterate over all commits.
        all_tags (bool): iterate over all available tags.
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

    from dvc.fs.local import LocalFileSystem

    saved_fs = self.fs

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
    )

    try:
        from dvc.fs.git import GitFileSystem

        for sha, names in found_revs.items():
            self.fs = GitFileSystem(scm=scm, rev=sha)
            # ignore revs that don't contain repo root
            # (i.e. revs from before a subdir=True repo was init'ed)
            if self.fs.exists(self.root_dir):
                yield sha if sha_only else ",".join(names)
    finally:
        self.fs = saved_fs
