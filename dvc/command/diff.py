import argparse
import json
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdDiff(CmdBase):
    def run(self):
        try:
            diff = self.repo.diff(
                self.args.a_ref, self.args.b_ref, target=self.args.target
            )
            if not any(diff.values()):
                return 0

            if self.args.json:
                print(json.dumps(diff))
                return 0

        except DvcException:
            logger.exception("failed to get 'diff {}'")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    DIFF_DESCRIPTION = (
        "Show diff of a data file or a directory that is under DVC control.\n"
        "Some basic statistics summary, how many files were deleted/changed."
    )
    DIFF_HELP = "Show a diff of a DVC controlled data file or a directory."
    diff_parser = subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(DIFF_DESCRIPTION, "diff"),
        help=DIFF_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    diff_parser.add_argument(
        "a_ref",
        help="Git reference from which diff calculates (defaults to HEAD)",
        nargs="?",
        default="HEAD",
    )
    diff_parser.add_argument(
        "b_ref",
        help=(
            "Git reference until which diff calculates, if omitted "
            "diff shows the difference between the working tree and a_ref"
        ),
        nargs="?",
    )
    diff_parser.add_argument(
        "-t",
        "--target",
        help=(
            "Source path to a data file or directory. Default None. "
            "If not specified, compares all files and directories "
            "that are under DVC control in the current working space."
        ),
    )
    diff_parser.add_argument(
        "--json",
        help=("Format the output into a JSON"),
        action="store_true",
        default=False,
    )
    diff_parser.set_defaults(func=CmdDiff)
