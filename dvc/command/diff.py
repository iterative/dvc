from __future__ import unicode_literals

from dvc.command.base import CmdBase


class CmdMove(CmdBase):
    def run(self):
        self.repo.diff(
            self.args.target, a_ref=self.args.a_ref, b_ref=self.args.b_ref
        )
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
        "target", help="Source path to a data file or directory."
    )
    diff_parser.add_argument(
        "a_ref", help="Git tag/reference from which diff calculates"
    )
    diff_parser.add_argument(
        "b_ref",
        help="Git tag/reference till which diff calculates",
        nargs="?",
        default=None,
    )
    diff_parser.set_defaults(func=CmdMove)
