import os

import dvc.logger as logger
from dvc.utils.compat import urlparse
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdImport(CmdBase):
    def run(self):
        try:
            default_out = os.path.basename(
                urlparse(self.args.url).path
            )

            out = self.args.out or default_out

            self.project.imp(self.args.url, out)
        except DvcException:
            logger.error('failed to import {}. You could also try downloading '
                         'it manually and adding it with `dvc add` command.'
                         .format(self.args.url))
            return 1
        return 0
