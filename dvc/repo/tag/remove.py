import logging

from dvc.repo import locked
from dvc.dvcfile import Dvcfile


logger = logging.getLogger(__name__)


@locked
def remove(self, tag, target=None, with_deps=False, recursive=False):
    stages = self.collect(target, with_deps=with_deps, recursive=recursive)
    for stage in stages:
        changed = False
        for out in stage.outs:
            if tag not in out.tags.keys():
                logger.warning("tag '{}' not found for '{}'".format(tag, out))
                continue
            del out.tags[tag]
            changed = True
        if changed:
            dvcfile = Dvcfile(self, stage.path)
            dvcfile.dump(stage)
