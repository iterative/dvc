from __future__ import unicode_literals

import humanize
import inflect


import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.utils.collections import compact
import dvc.scm.base as git


class CmdDiff(CmdBase):
    def _print_size(self, size):
        if size < 0:
            change = "decreased by {}"
        elif size > 0:
            change = "increased by {}"
        else:
            change = "not changed"
        natur_size = humanize.naturalsize(abs(size))
        return change.format(natur_size)

    def _show(self, diff_dct):
        engine = inflect.engine()
        msg = "dvc diff from {} to {}".format(
            diff_dct[git.DIFF_A_REF], diff_dct[git.DIFF_B_REF]
        )
        if diff_dct.get(git.DIFF_EQUAL):
            logger.info(msg)
            return
        for dct in diff_dct[git.DIFF_LIST]:
            msg += "\n\ndiff for '{}' \n".format(dct[git.DIFF_TARGET])
            if dct.get(git.DIFF_OLD_FILE):
                msg += "-{} with md5 {}\n".format(
                    dct[git.DIFF_OLD_FILE], dct[git.DIFF_OLD_CHECKSUM]
                )
            if dct.get(git.DIFF_NEW_FILE):
                msg += "+{} with md5 {}\n".format(
                    dct[git.DIFF_NEW_FILE], dct[git.DIFF_NEW_CHECKSUM]
                )
            msg += "\n"
            if dct[git.DIFF_SIZE] != git.DIFF_SIZE_UNKNOWN:
                if dct.get("is_dir"):
                    msg += "{} {} not changed, ".format(
                        dct[git.DIFF_IDENT],
                        engine.plural("file", dct[git.DIFF_IDENT]),
                    )
                    msg += "{} {} modified, ".format(
                        dct[git.DIFF_CHANGE],
                        engine.plural("file", dct[git.DIFF_CHANGE]),
                    )
                    msg += "{} {} added, ".format(
                        dct[git.DIFF_NEW],
                        engine.plural("file", dct[git.DIFF_NEW]),
                    )
                    msg += "{} {} deleted, ".format(
                        dct[git.DIFF_DEL],
                        engine.plural("file", dct[git.DIFF_DEL]),
                    )
                    msg += "size was {}".format(
                        self._print_size(dct[git.DIFF_SIZE])
                    )
                else:
                    if (
                        dct.get(git.DIFF_OLD_FILE)
                        and dct.get(git.DIFF_NEW_FILE)
                        and dct[git.DIFF_SIZE] == 0
                    ):
                        msg += "file size was not changed"
                    elif dct.get(git.DIFF_NEW_FILE):
                        msg += "added file with size {}".format(
                            humanize.naturalsize(dct[git.DIFF_SIZE])
                        )
                    elif dct.get(git.DIFF_OLD_FILE):
                        msg += "deleted file with size {}".format(
                            humanize.naturalsize(abs(dct[git.DIFF_SIZE]))
                        )
                    else:
                        msg += "file was modified, file size {}".format(
                            self._print_size(dct[git.DIFF_SIZE])
                        )
            else:
                msg += "size is ?"
        logger.info(msg)

    def run(self):
        try:
            msg = self.repo.diff(
                self.args.a_ref, target=self.args.target, b_ref=self.args.b_ref
            )
            self._show(msg)
        except DvcException:
            msg = "failed to get 'diff {}'"
            args = " ".join(
                compact([self.args.target, self.args.a_ref, self.args.b_ref])
            )
            msg = msg.format(args)
            logger.error(msg)
            return 1
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
