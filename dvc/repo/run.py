from typing import TYPE_CHECKING, Union

from . import locked
from .scm_context import scm_context

if TYPE_CHECKING:
    from dvc.stage import PipelineStage, Stage

    from . import Repo


@locked
@scm_context
def run(
    self: "Repo",
    no_exec: bool = False,
    no_commit: bool = False,
    run_cache: bool = True,
    force: bool = True,
    **kwargs
) -> Union["Stage", "PipelineStage", None]:
    from dvc.stage.utils import validate_state

    stage = self.stage.create_from_cli(**kwargs)

    validate_state(self, stage, force=force)

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run(no_commit=no_commit, run_cache=run_cache)

    stage.dump(update_lock=not no_exec)
    return stage
