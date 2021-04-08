import logging
import typing

from dvc.exceptions import InvalidArgumentError

from . import locked

if typing.TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)


@locked
def remove(self: "Repo", target: str, outs: bool = False):
    stages_info = self.stage.collect_granular(target)

    for stage, filter_info in stages_info:
        if filter_info is not None:
            for out in stage.filter_outs(filter_info):
                if out.path_info != filter_info:
                    raise InvalidArgumentError(
                        f"DVC is already tracking {out.path_info} "
                        f"and can't remove files within that directory."
                        f"\nYou can run 'dvc remove {out.path_info}' "
                        f"in order to remove {filter_info} and all "
                        f"the other files within that directory."
                    )
                out.remove(True)
        else:
            stage.remove(remove_outs=outs, force=outs)

    return [x.stage for x in stages_info]
