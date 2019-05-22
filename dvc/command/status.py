from __future__ import unicode_literals

import logging

from dvc.command.data_sync import CmdDataBase
from dvc.utils.compat import str


logger = logging.getLogger(__name__)


class CmdDataStatus(CmdDataBase):
    STATUS_LEN = 20
    STATUS_INDENT = "\t"
    UP_TO_DATE_MSG = "Pipeline is up to date. Nothing to reproduce."

    def _normalize(self, s):
        s += ":"
        assert len(s) < self.STATUS_LEN
        return s + (self.STATUS_LEN - len(s)) * " "

    def _show(self, status, indent=0):
        ind = indent * self.STATUS_INDENT

        if isinstance(status, str):
            logger.info("{}{}".format(ind, status))
            return

        if isinstance(status, list):
            for entry in status:
                self._show(entry, indent)
            return

        assert isinstance(status, dict)

        for key, value in status.items():
            if isinstance(value, str):
                logger.info("{}{}{}".format(ind, self._normalize(value), key))
            elif value:
                logger.info("{}{}:".format(ind, key))
                self._show(value, indent + 1)

    def do_run(self, target=None):
        indent = 1 if self.args.cloud else 0
        try:
            st = self.repo.status(
                target=target,
                jobs=self.args.jobs,
                cloud=self.args.cloud,
                show_checksums=self.args.show_checksums,
                remote=self.args.remote,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                with_deps=self.args.with_deps,
            )
            if st:
                if self.args.quiet:
                    return 1
                else:
                    self._show(st, indent)
            else:
                logger.info(self.UP_TO_DATE_MSG)

        except Exception:
            logger.exception("failed to obtain data status")
            return 1
        return 0
