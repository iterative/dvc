from __future__ import unicode_literals

import argparse
import logging

import humanize
import inflect
from funcy import compact

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdDiff(CmdBase):
    @staticmethod
    def _print_size(size):
        if size < 0:
            change = "decreased by {}"
        elif size > 0:
            change = "increased by {}"
        else:
            change = "not changed"
        natur_size = humanize.naturalsize(abs(size))
        return change.format(natur_size)

    @staticmethod
    def _get_md5_string(sign, file_name, checksum):
        sample_msg = ""
        if file_name:
            sample_msg = "{}{} with md5 {}\n"
            sample_msg = sample_msg.format(sign, file_name, checksum)
        return sample_msg

    @classmethod
    def _get_dir_changes(cls, dct):
        import dvc.repo.diff as diff

        engine = inflect.engine()
        changes_msg = (
            "{} {} untouched, {} {} modified, {} {} added, "
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
            cls._print_size(dct[diff.DIFF_SIZE]),
        )
        return changes_msg

    @classmethod
    def _get_file_changes(cls, dct):
        import dvc.repo.diff as diff

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
                cls._print_size(dct[diff.DIFF_SIZE])
            )
        return msg

    @classmethod
    def _get_royal_changes(cls, dct):
        import dvc.repo.diff as diff

        if dct[diff.DIFF_SIZE] != diff.DIFF_SIZE_UNKNOWN:
            if dct.get("is_dir"):
                return cls._get_dir_changes(dct)
            else:
                return cls._get_file_changes(dct)
        return "size is ?"

    @classmethod
    def _show(cls, diff_dct):
        import dvc.repo.diff as diff

        msg = "dvc diff from {} to {}".format(
            diff_dct[diff.DIFF_A_REF], diff_dct[diff.DIFF_B_REF]
        )
        if diff_dct.get(diff.DIFF_EQUAL):
            logger.info(msg)
            return
        for dct in diff_dct[diff.DIFF_LIST]:
            msg += "\n\ndiff for '{}'\n".format(dct[diff.DIFF_TARGET])
            msg += cls._get_md5_string(
                "-",
                dct.get(diff.DIFF_OLD_FILE),
                dct.get(diff.DIFF_OLD_CHECKSUM),
            )
            msg += cls._get_md5_string(
                "+",
                dct.get(diff.DIFF_NEW_FILE),
                dct.get(diff.DIFF_NEW_CHECKSUM),
            )
            msg += "\n"
            msg += cls._get_royal_changes(dct)
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
            "Git reference until which diff calculates, if omitted "
            "diff shows the difference between current HEAD and a_ref"
        ),
        nargs="?",
    )
    diff_parser.set_defaults(func=CmdDiff)
