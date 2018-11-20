import os

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.logger import Logger


class CmdImport(CmdBase):
    def run(self):
        try:
            default_out = os.path.basename(
                urlparse(self.args.url).path
            )

            out = self.args.out or default_out

            self.project.imp(self.args.url, out)
        except DvcException as ex:
            Logger.error('Failed to import {}'.format(self.args.url), ex)
            return 1
        return 0
