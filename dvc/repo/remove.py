import logging

from ..utils import parse_target
from . import locked

logger = logging.getLogger(__name__)


@locked
def remove(self, target, dvc_only=False):
    from ..dvcfile import Dvcfile

    path, name = parse_target(target)
    dvcfile = Dvcfile(self, path)
    stages = list(dvcfile.stages.filter(name).values())
    for stage in stages:
        stage.remove_outs(force=True)

    if not dvc_only:
        dvcfile.remove()

    return stages
