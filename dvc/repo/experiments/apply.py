import logging
import os
from typing import TYPE_CHECKING, Optional

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import Git
from dvc.ui import ui
from dvc.utils.fs import remove

from .exceptions import BaselineMismatchError, InvalidExpRevError
from .executor.base import BaseExecutor
from .refs import EXEC_APPLY

if TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.repo.experiments import Experiments

logger = logging.getLogger(__name__)


@locked
@scm_context
def apply(repo: "Repo", rev: str, **kwargs):  # noqa: C901
    from dvc.repo.checkout import checkout as dvc_checkout
    from dvc.scm import RevError, resolve_rev

    exps: "Experiments" = repo.experiments

    is_stash: bool = False

    assert isinstance(repo.scm, Git)
    try:
        exp_rev = resolve_rev(repo.scm, rev)
    except RevError as exc:
        (
            exp_ref_info,
            queue_entry,
        ) = exps.celery_queue.get_ref_and_entry_by_names(
            rev
        )[rev]
        if exp_ref_info:
            exp_rev = repo.scm.get_ref(str(exp_ref_info))
        elif queue_entry:
            exp_rev = queue_entry.stash_rev
            is_stash = True
        else:
            raise InvalidExpRevError(rev) from exc
    except BaselineMismatchError as exc:
        raise InvalidExpRevError(rev) from exc

    _apply(repo, exp_rev, name=rev, is_stash=is_stash)
    dvc_checkout(repo, **kwargs)

    repo.scm.set_ref(EXEC_APPLY, exp_rev)
    ui.write(
        f"Changes for experiment '{rev}' have been applied to your current workspace.",
    )


def _apply(repo: "Repo", rev: str, name: Optional[str] = None, is_stash: bool = False):
    exps: "Experiments" = repo.experiments

    with exps.apply_stash.preserve_workspace(rev, name=name):
        with repo.scm.detach_head(rev, force=True):
            if is_stash:
                assert repo.tmp_dir is not None
                args_path = os.path.join(repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)
                if os.path.exists(args_path):
                    remove(args_path)
