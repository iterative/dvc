import logging

from . import locked
from ..utils import parse_target

logger = logging.getLogger(__name__)


@locked
def remove(self, target, outs_only=False):
    from ..dvcfile import Dvcfile

    path, name = parse_target(target)
    dvcfile = Dvcfile(self, path)
    stages = list(dvcfile.stages.filter(name).values())
    for stage in stages:
        stage.remove_outs(force=True)

    if not outs_only:
        dvcfile.remove()

    return stages
