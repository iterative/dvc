from typing import TYPE_CHECKING

from . import locked
from .scm_context import scm_context

if TYPE_CHECKING:
    from . import Repo


@locked
@scm_context
def run(
    self: "Repo",
    fname: str = None,
    no_exec: bool = False,
    single_stage: bool = False,
    **kwargs
):
    from dvc.stage.utils import check_graphs, create_stage_from_cli

    stage = create_stage_from_cli(
        self, single_stage=single_stage, fname=fname, **kwargs
    )

    if kwargs.get("run_cache", True) and stage.can_be_skipped:
        return None

    check_graphs(self, stage, force=kwargs.get("force", True))

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run(
            no_commit=kwargs.get("no_commit", False),
            run_cache=kwargs.get("run_cache", True),
        )

    stage.dump(update_lock=not no_exec)
    return stage
