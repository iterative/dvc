import logging

from ..utils import parse_target
from . import locked

logger = logging.getLogger(__name__)


@locked
def remove(self, target, outs=False):
    path, name = parse_target(target)
    stages = self.get_stages(path, name)

    for stage in stages:
        stage.remove(remove_outs=outs, force=outs)

    return stages
