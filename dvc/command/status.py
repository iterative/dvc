import logging

from dvc.command.data_sync import CmdDataBase

logger = logging.getLogger(__name__)


class CmdDataStatus(CmdDataBase):
    STATUS_LEN = 20
    STATUS_INDENT = "\t"
    UP_TO_DATE_MSG = "Data and pipelines are up to date."

    def _normalize(self, s):
        s += ":"
        assert len(s) < self.STATUS_LEN
        return s + (self.STATUS_LEN - len(s)) * " "

    def _show(self, status, indent=0):
        ind = indent * self.STATUS_INDENT

        if isinstance(status, str):
            logger.info(f"{ind}{status}")
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
                logger.info(f"{ind}{key}:")
                self._show(value, indent + 1)

    def run(self):
        indent = 1 if self.args.cloud else 0
        try:
            st = self.repo.status(
                targets=self.args.targets,
                jobs=self.args.jobs,
                cloud=self.args.cloud,
                remote=self.args.remote,
                all_branches=self.args.all_branches,
                all_tags=self.args.all_tags,
                all_commits=self.args.all_commits,
                with_deps=self.args.with_deps,
                recursive=self.args.recursive,
            )

            if self.args.quiet:
                return bool(st)

            if self.args.show_json:
                import json

                logger.info(json.dumps(st))
            elif st:
                self._show(st, indent)
            else:
                logger.info(self.UP_TO_DATE_MSG)

        except Exception:  # noqa, pylint: disable=broad-except
            logger.exception("failed to obtain data status")
            return 1
        return 0
