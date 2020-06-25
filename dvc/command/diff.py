import argparse
import json
import logging
import os

import colorama

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


def _show_md(diff):
    from dvc.utils.diff import table

    rows = []
    for status in ["added", "deleted", "modified"]:
        entries = diff.get(status, [])
        if not entries:
            continue
        paths = sorted(entry["path"] for entry in entries)
        for path in paths:
            rows.append([status, path])

    return table(["Status", "Path"], rows, True)


class CmdDiff(CmdBase):
    @staticmethod
    def _format(diff):
        """
        Given a diff structure, generate a string of paths separated
        by new lines and grouped together by their state.

        A group's header is colored and its entries are sorted to enhance
        readability, for example:

            Added:
                another_file.txt
                backup.tar
                dir/
                dir/1

        An example of a diff formatted when entries contain hash:

            Added:
                d3b07384 foo

            Modified:
                c157a790..f98bf6f1 bar

        If a group has no entries, it won't be included in the result.

        At the bottom, include a summary with the number of files per state.
        """

        def _digest(checksum):
            if isinstance(checksum, str):
                return checksum[0:8]
            return "{}..{}".format(checksum["old"][0:8], checksum["new"][0:8])

        colors = {
            "added": colorama.Fore.GREEN,
            "modified": colorama.Fore.YELLOW,
            "deleted": colorama.Fore.RED,
        }

        summary = {}
        groups = []

        for state in ["added", "deleted", "modified"]:
            summary[state] = 0
            entries = diff[state]

            if not entries:
                continue

            content = []

            for entry in entries:
                path = entry["path"]
                checksum = entry.get("hash")
                summary[state] += 1 if not path.endswith(os.sep) else 0
                content.append(
                    "{space}{checksum}{separator}{path}".format(
                        space="    ",
                        checksum=_digest(checksum) if checksum else "",
                        separator="  " if checksum else "",
                        path=entry["path"],
                    )
                )

            groups.append(
                "{color}{header}{nc}:\n{content}".format(
                    color=colors[state],
                    header=state.capitalize(),
                    nc=colorama.Fore.RESET,
                    content="\n".join(content),
                )
            )

        groups.append(
            "files summary: {added} added, {deleted} deleted,"
            " {modified} modified".format_map(summary)
        )

        return "\n\n".join(groups)

    def run(self):
        try:
            diff = self.repo.diff(self.args.a_rev, self.args.b_rev)

            if not self.args.show_hash:
                for _, entries in diff.items():
                    for entry in entries:
                        del entry["hash"]

            if self.args.show_json:
                logger.info(json.dumps(diff))
            elif self.args.show_md:
                logger.info(_show_md(diff))
            elif diff:
                logger.info(self._format(diff))

        except DvcException:
            logger.exception("failed to get diff")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    DIFF_DESCRIPTION = (
        "Show added, modified, or deleted data between commits in the DVC"
        " repository, or between a commit and the workspace."
    )
    diff_parser = subparsers.add_parser(
        "diff",
        parents=[parent_parser],
        description=append_doc_link(DIFF_DESCRIPTION, "diff"),
        help=DIFF_DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    diff_parser.add_argument(
        "a_rev",
        help="Old Git commit to compare (defaults to HEAD)",
        nargs="?",
        default="HEAD",
    )
    diff_parser.add_argument(
        "b_rev",
        help=("New Git commit to compare (defaults to the current workspace)"),
        nargs="?",
    )
    diff_parser.add_argument(
        "--show-json",
        help="Format the output into a JSON",
        action="store_true",
        default=False,
    )
    diff_parser.add_argument(
        "--show-hash",
        help="Display hash value for each entry",
        action="store_true",
        default=False,
    )
    diff_parser.add_argument(
        "--show-md",
        help="Show tabulated output in the Markdown format (GFM).",
        action="store_true",
        default=False,
    )
    diff_parser.set_defaults(func=CmdDiff)
