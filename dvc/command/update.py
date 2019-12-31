import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdUpdate(CmdBase):
    def run(self):
        ret = 0
        for target in self.args.targets:
            try:
                self.repo.update(target)
            except DvcException:
                logger.exception("failed to update '{}'.".format(target))
                ret = 1
        return ret


def add_parser(subparsers, parent_parser):
    UPDATE_HELP = "Update data artifacts imported from other DVC repositories."
    update_parser = subparsers.add_parser(
        "update",
        parents=[parent_parser],
        description=append_doc_link(UPDATE_HELP, "update"),
        help=UPDATE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    update_parser.add_argument(
        "targets", nargs="+", help="DVC-files to update."
    )
    update_parser.set_defaults(func=CmdUpdate)
