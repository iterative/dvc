import argparse
import logging

import dvc.prompt as prompt
from dvc.command.base import CmdBase, append_doc_link

logger = logging.getLogger(__name__)


class CmdDestroy(CmdBase):
    def run(self):
        from dvc.exceptions import DvcException

        try:
            statement = (
                "This will destroy all information about your pipelines,"
                " all data files, as well as cache in .dvc/cache."
                "\n"
                "Are you sure you want to continue?"
            )

            if not self.args.force and not prompt.confirm(statement):
                raise DvcException(
                    "cannot destroy without a confirmation from the user."
                    " Use `-f` to force."
                )

            self.repo.destroy()
        except DvcException:
            logger.exception("failed to destroy DVC")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    DESTROY_HELP = (
        "Terminate the DVC project by removing all DVC files."
        " Data files and directories will remain."
    )

    destroy_parser = subparsers.add_parser(
        "destroy",
        parents=[parent_parser],
        description=append_doc_link(DESTROY_HELP, "destroy"),
        help=DESTROY_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    destroy_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force destruction.",
    )
    destroy_parser.set_defaults(func=CmdDestroy)
