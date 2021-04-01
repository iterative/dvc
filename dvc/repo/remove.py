import logging
import typing

from . import locked

if typing.TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


@locked
def remove(self: "Repo", target: str, outs: bool = False):
    stages_info = self.stage.collect_granular(target)

    for stage_info in stages_info:
        stage = stage_info.stage
        filter_info = stage_info.filter_info
        if filter_info is not None:
            # target is a specific output file
            for out in stage.filter_outs(filter_info):
                out.remove(True)
        else:
            stage.remove(remove_outs=outs, force=outs)

    return [x.stage for x in stages_info]
