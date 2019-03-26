from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


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
            msg = "failed to get diff "
            if self.args.target:
                msg += self.args.target + " "
            if self.args.a_ref:
                msg += self.args.a_ref + " "
            if self.args.b_ref:
                msg += self.args.b_ref + " "
            logger.error(msg)
        return 0


def add_parser(subparsers, parent_parser):
    description = (
        "show diff of a data file or a directory that "
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
            "if None shows the diff between all DVC tracked files/directories"
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
