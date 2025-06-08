from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.metrics.diff import DiffResult


def diff(
    repo: "Repo",
    a_rev: str = "HEAD",
    b_rev: str = "workspace",
    all: bool = False,  # noqa: A002
    **kwargs,
) -> "DiffResult":
    if repo.scm.no_commits:
        return {}

    from dvc.repo.metrics.diff import _diff

    params = repo.params.show(revs=[a_rev, b_rev], hide_workspace=False, **kwargs)
    return _diff(params, a_rev, b_rev, with_unchanged=all)
