from __future__ import unicode_literals

import argparse

import humanize
import inflect
import logging

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, append_doc_link
from dvc.utils.collections import compact
import dvc.repo.diff as diff


logger = logging.getLogger(__name__)


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

    def _get_md5_string(self, sign, file_name, checksum):
        sample_msg = ""
        if file_name:
            sample_msg = "{}{} with md5 {}\n"
            sample_msg = sample_msg.format(sign, file_name, checksum)
        return sample_msg

    def _get_dir_changes(self, dct):
        engine = inflect.engine()
        changes_msg = (
            "{} {} not changed, {} {} modified, {} {} added, "
            "{} {} deleted, size was {}"
        )
        changes_msg = changes_msg.format(
            dct[diff.DIFF_IDENT],
            engine.plural("file", dct[diff.DIFF_IDENT]),
            dct[diff.DIFF_CHANGE],
            engine.plural("file", dct[diff.DIFF_CHANGE]),
            dct[diff.DIFF_NEW],
            engine.plural("file", dct[diff.DIFF_NEW]),
            dct[diff.DIFF_DEL],
            engine.plural("file", dct[diff.DIFF_DEL]),
            self._print_size(dct[diff.DIFF_SIZE]),
        )
        return changes_msg

    def _get_file_changes(self, dct):
        if (
            dct.get(diff.DIFF_OLD_FILE)
            and dct.get(diff.DIFF_NEW_FILE)
            and dct[diff.DIFF_SIZE] == 0
        ):
            msg = "file size was not changed"
        elif dct.get(diff.DIFF_NEW_FILE):
            msg = "added file with size {}".format(
                humanize.naturalsize(dct[diff.DIFF_SIZE])
            )
        elif dct.get(diff.DIFF_OLD_FILE):
            msg = "deleted file with size {}".format(
                humanize.naturalsize(abs(dct[diff.DIFF_SIZE]))
            )
        else:
            msg = "file was modified, file size {}".format(
                self._print_size(dct[diff.DIFF_SIZE])
            )
        return msg

    def _get_royal_changes(self, dct):
        if dct[diff.DIFF_SIZE] != diff.DIFF_SIZE_UNKNOWN:
            if dct.get("is_dir"):
                return self._get_dir_changes(dct)
            else:
                return self._get_file_changes(dct)
        return "size is ?"

    def _show(self, diff_dct):
        msg = "dvc diff from {} to {}".format(
            diff_dct[diff.DIFF_A_REF], diff_dct[diff.DIFF_B_REF]
        )
        if diff_dct.get(diff.DIFF_EQUAL):
            logger.info(msg)
            return
        for dct in diff_dct[diff.DIFF_LIST]:
            msg += "\n\ndiff for '{}'\n".format(dct[diff.DIFF_TARGET])
            msg += self._get_md5_string(
                "-",
                dct.get(diff.DIFF_OLD_FILE),
                dct.get(diff.DIFF_OLD_CHECKSUM),
            )
            msg += self._get_md5_string(
                "+",
                dct.get(diff.DIFF_NEW_FILE),
                dct.get(diff.DIFF_NEW_CHECKSUM),
            )
            msg += "\n"
            msg += self._get_royal_changes(dct)
        logger.info(msg)
        return msg

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
            logger.exception(msg)
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
        "-t",
        "--target",
        default=None,
        help=(
            "Source path to a data file or directory. Default None. "
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
            "Git reference untill which diff calculates, if omitted "
            "diff shows the difference between current HEAD and a_ref"
        ),
        nargs="?",
        default=None,
    )
    diff_parser.set_defaults(func=CmdDiff)
