from __future__ import unicode_literals

import humanize
import inflect


import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.utils.collections import compact


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
            diff_dct["a_ref"], diff_dct["b_ref"]
        )
        if diff_dct.get("equal"):
            logger.info(msg)
            return
        for dct in diff_dct["diffs"]:
            msg += "\n\ndiff for '{}' \n".format(dct["target"])
            if dct.get("old_file"):
                msg += "-{} with md5 {}\n".format(
                    dct["old_file"], dct["old_checksum"]
                )
            if dct.get("new_file"):
                msg += "+{} with md5 {}\n".format(
                    dct["new_file"], dct["new_checksum"]
                )
            msg += "\n"
            if dct.get("is_dir"):
                msg += "{} {} not changed, ".format(
                    dct["ident"], engine.plural("file", dct["ident"])
                )
                msg += "{} {} modified, ".format(
                    dct["changes"], engine.plural("file", dct["changes"])
                )
                msg += "{} {} added, ".format(
                    dct["new"], engine.plural("file", dct["new"])
                )
                msg += "{} {} deleted, ".format(
                    dct["del"], engine.plural("file", dct["del"])
                )
                msg += "size was {}".format(self._print_size(dct["size_diff"]))
            else:
                if (
                    dct.get("old_file")
                    and dct.get("new_file")
                    and dct["size_diff"] == 0
                ):
                    msg += "file size was not changed"
                elif dct.get("new_file"):
                    msg += "added file with size {}".format(
                        humanize.naturalsize(dct["size_diff"])
                    )
                elif dct.get("old_file"):
                    msg += "deleted file with size {}".format(
                        humanize.naturalsize(abs(dct["size_diff"]))
                    )
                else:
                    msg += "file was modified, file size {}".format(
                        self._print_size(dct["size_diff"])
                    )
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
