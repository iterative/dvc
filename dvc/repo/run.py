from typing import TYPE_CHECKING, List, Union

from dvc.utils.cli_parse import parse_params

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
    params: List[str] = None,
    **kwargs
) -> Union["Stage", "PipelineStage"]:

    kwargs.update(
        {
            "from_cli": True,
            "force": force,
            "params": parse_params(params or []),
        }
    )
    stage = self.stage.create(**kwargs)

    if no_exec:
        stage.ignore_outs()
    else:
        stage.run(no_commit=no_commit, run_cache=run_cache)

    stage.dump(update_lock=not no_exec)
    return stage
