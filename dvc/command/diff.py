from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.utils.collections import compact


class CmdDiff(CmdBase):
    def _show(self, msg):
        logger.info(msg)

    def run(self):
        try:
            msg = self.repo.diff(
                self.args.target, a_ref=self.args.a_ref, b_ref=self.args.b_ref
            )
            self._show(msg)
        except DvcException:
            msg = "failed to get 'diff "
            msg += " ".join(
                compact([self.args.target, self.args.a_ref, self.args.b_ref])
            )
            msg += "'"
            logger.error(msg)
        return 0


def add_parser(subparsers, parent_parser):
    description = (
        "Show diff of a data file or a directory that "
        "is under DVC control. Some basic statistics "
        "summary, how many files were deleted/changed."
    )
    help = "Show a diff of a DVC controlled data file or a directory."
    diff_parser = subparsers.add_parser(
        "diff", parents=[parent_parser], description=description, help=help
    )
    diff_parser.add_argument(
        "-t",
        "--target",
        default=None,
        help=(
            "Source path to a data file or directory. Default None,"
            "If not specified, compares all files and directories "
            "that are under DVC control in the current working space."
        ),
    )
    diff_parser.add_argument(
        "a_ref", help="Git reference from which diff calculates"
    )
    diff_parser.add_argument(
        "b_ref",
        help=(
            "Git reference till which diff calculates, if omitted "
            "diff shows the difference between current HEAD and a_ref"
        ),
        nargs="?",
        default=None,
    )
    diff_parser.set_defaults(func=CmdDiff)
