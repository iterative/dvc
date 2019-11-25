from funcy import group_by

from dvc.scm.tree import WorkingTree


def brancher(  # noqa: E302
    self,
    branches=None,
    all_branches=False,
    tags=None,
    all_tags=False,
    all_commits=False,
):
    """Generator that iterates over specified revisions.

    Args:
        branches (list): a list of branches to iterate over.
        all_branches (bool): iterate over all available branches.
        tags (list): a list of tags to iterate over.
        all_tags (bool): iterate over all available tags.

    Yields:
        str: the display name for the currently selected tree, it could be:
            - a git revision identifier
            - empty string it there is no branches to iterate over
            - "Working Tree" if there are uncommitted changes in the SCM repo
    """
    if not any([branches, all_branches, tags, all_tags, all_commits]):
        yield ""
        return

    saved_tree = self.tree
    revs = []

    scm = self.scm

    self.tree = WorkingTree(self.root_dir)
    yield "working tree"

    if all_commits:
        revs = scm.list_all_commits()
    else:
        if all_branches:
            branches = scm.list_branches()

        if all_tags:
            tags = scm.list_tags()

        if branches is not None:
            revs.extend(branches)

        if tags is not None:
            revs.extend(tags)

    # NOTE: it might be a good idea to wrap this loop in try/finally block
    # to don't leave the tree on some unexpected branch after the
    # `brancher()`, but this could cause problems on exception handling
    # code which might expect the tree on which exception was raised to
    # stay in place. This behavior is a subject to change.
    for sha, names in group_by(scm.resolve_rev, revs).items():
        self.tree = scm.get_tree(sha)
        yield ", ".join(names)

    self.tree = saved_tree
