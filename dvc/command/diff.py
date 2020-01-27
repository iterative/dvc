import argparse
import json
import logging

import colorama

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdDiff(CmdBase):
    @staticmethod
    def _format(diff):
        """
        Given a diff structure, generate a string of filenames separated
        by new lines and grouped together by their state.

        A group's header is colored and its entries are sorted to enhance
        readability, for example:

            Added:
                another_file.txt
                backup.tar
                dir/
                dir/1

        An example of a diff formatted when entries contain checksums:

            Added:
                d3b07384 foo

            Modified:
                c157a790..f98bf6f1 bar

        If a group has no entries, it won't be included in the result.
        """

        def _digest(checksum):
            if not checksum:
                return ""
            if type(checksum) is str:
                return checksum[0:8]
            return "{}..{}".format(checksum["old"][0:8], checksum["new"][0:8])

        colors = {
            "added": colorama.Fore.GREEN,
            "modified": colorama.Fore.YELLOW,
            "deleted": colorama.Fore.RED,
        }

        return "\n\n".join(
            "{color}{header}{nc}:\n{entries}".format(
                color=colors[state],
                header=state.capitalize(),
                nc=colorama.Fore.RESET,
                entries="\n".join(
                    "{space}{checksum}{separator}{filename}".format(
                        space="    ",
                        checksum=_digest(entry.get("checksum")),
                        separator="  " if entry.get("checksum") else "",
                        filename=entry["filename"],
                    )
                    for entry in entries
                ),
            )
            for state, entries in diff.items()
            if entries
        )

    def run(self):
        try:
            diff = self.repo.diff(self.args.a_ref, self.args.b_ref)

            if not any(diff.values()):
                return 0

            if not self.args.checksums:
                for _, entries in diff.items():
                    for entry in entries:
                        del entry["checksum"]

            if self.args.show_json:
                res = json.dumps(diff)
            else:
                res = self._format(diff)

            logger.info(res)

        except DvcException:
            logger.exception("failed to get diff")
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
        "--show-json",
        help="Format the output into a JSON",
        action="store_true",
        default=False,
    )
    diff_parser.add_argument(
        "--checksums",
        help="Display checksums for each entry",
        action="store_true",
        default=False,
    )
    diff_parser.set_defaults(func=CmdDiff)
