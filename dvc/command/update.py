import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdUpdate(CmdBase):
    def run(self):
        ret = 0
        try:
            self.repo.update(
                targets=self.args.targets,
                rev=self.args.rev,
                recursive=self.args.recursive,
            )
        except DvcException:
            logger.exception("failed update data")
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
        "targets", nargs="+", help="DVC-files to update.",
    ).complete = completion.DVC_FILE
    update_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    update_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Update all stages in the specified directory.",
    )
    update_parser.set_defaults(func=CmdUpdate)
