import logging

from ..utils import parse_target
from . import locked

logger = logging.getLogger(__name__)


@locked
def remove(self, target, dvc_only=False):
    from ..dvcfile import Dvcfile, is_valid_filename

    path, name = parse_target(target)
    stages = self.get_stages(path, name)
    for stage in stages:
        stage.remove_outs(force=True)

    if path and is_valid_filename(path) and not dvc_only:
        Dvcfile(self, path).remove()

    return stages
