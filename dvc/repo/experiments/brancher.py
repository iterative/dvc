from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING

from dvc.repo.experiments.exceptions import InvalidExpRevError
from dvc.scm import RevError

if TYPE_CHECKING:
    from dvc.repo import Repo


@contextmanager
def switch_repo(repo: "Repo", rev: str) -> Iterator[tuple["Repo", str]]:
    """Return a repo instance (brancher) switched to rev.

    If rev is the name of a running experiment, the returned instance will be
    the live repo wherever the experiment is running.

    NOTE: This will not resolve git SHA's that only exist in queued exp workspaces
    (it will only match queued exp names).
    """
    try:
        with repo.switch(rev):
            yield repo, rev
        return
    except RevError as exc:
        orig_exc = exc
    exps = repo.experiments

    if rev == exps.workspace_queue.get_running_exp():
        yield repo, "workspace"
        return

    for queue in (exps.tempdir_queue, exps.celery_queue):
        try:
            active_repo = queue.active_repo(rev)
        except InvalidExpRevError:
            continue
        stack = ExitStack()
        stack.enter_context(active_repo)
        stack.enter_context(active_repo.switch("workspace"))
        with stack:
            yield active_repo, rev
        return
    raise orig_exc
