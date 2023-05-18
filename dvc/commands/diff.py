import argparse
import logging
import os

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


def _digest(checksum):
    if isinstance(checksum, str):
        return checksum[0:8]
    return "{}..{}".format(checksum["old"][0:8], checksum["new"][0:8])


def _show_markdown(diff, show_hash=False, hide_missing=False):
    headers = ["Status", "Hash", "Path"] if show_hash else ["Status", "Path"]
    rows = []
    statuses = ["added", "deleted", "renamed", "modified"]
    if not hide_missing:
        statuses.append("not in cache")

    for status in statuses:
        entries = diff.get(status, [])
        if not entries:
            continue
        for entry in entries:
            path = entry["path"]
            if isinstance(path, dict):
                path = f"{path['old']} -> {path['new']}"
            if show_hash:
                check_sum = _digest(entry.get("hash", ""))
                rows.append([status, check_sum, path])
            else:
                rows.append([status, path])

    ui.table(rows, headers=headers, markdown=True)


class CmdDiff(CmdBase):
    @staticmethod
    def _show_diff(diff, hide_missing=False):
        """
        Given a diff structure, generate a string of paths separated
        by new lines and grouped together by their state.

        A group's header is colored to enhance readability, for example:

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

        colors = {
            "added": "green",
            "modified": "yellow",
            "deleted": "red",
            "renamed": "green",
            "not in cache": "yellow",
        }

        summary = {}

        states = ["added", "deleted", "renamed", "modified"]
        if not hide_missing:
            states.append("not in cache")
        for state in states:
            summary[state] = 0
            entries = diff[state]

            if not entries:
                continue

            header = state.capitalize()
            ui.write(f"[{colors[state]}]{header}[/]:", styled=True)

            for entry in entries:
                path = entry["path"]
                if isinstance(path, dict):
                    path = f"{path['old']} -> {path['new']}"
                checksum = entry.get("hash")
                summary[state] += 1 if not path.endswith(os.sep) else 0
                ui.write(
                    "{space}{checksum}{separator}{path}".format(
                        space="    ",
                        checksum=_digest(checksum) if checksum else "",
                        separator="  " if checksum else "",
                        path=path,
                    )
                )

            ui.write()

        if not sum(summary.values()):
            return

        states_summary = ", ".join(
            f"{summary[state]} {state}" for state in states if summary[state] > 0
        )
        ui.write("files summary:", states_summary)

    def run(self):
        from dvc.exceptions import DvcException

        try:
            diff = self.repo.diff(self.args.a_rev, self.args.b_rev, self.args.targets)
            show_hash = self.args.show_hash
            hide_missing = self.args.b_rev or self.args.hide_missing
            if hide_missing:
                diff.pop("not in cache", None)

            for key, entries in diff.items():
                entries = sorted(
                    entries,
                    key=lambda entry: (
                        entry["path"]["old"]
                        if isinstance(entry["path"], dict)
                        else entry["path"]
                    ),
                )
                if not show_hash:
                    for entry in entries:
                        del entry["hash"]
                diff[key] = entries

            if self.args.json:
                ui.write_json(diff)
            elif self.args.markdown:
                _show_markdown(diff, show_hash, hide_missing)
            elif diff:
                self._show_diff(diff, hide_missing)

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
        "--targets",
        nargs="*",
        help="Specific DVC-tracked files to compare. Accepts one or more file paths.",
        metavar="<paths>",
    ).complete = completion.FILE
    diff_parser.add_argument(
        "a_rev",
        help="Old Git commit to compare (defaults to HEAD)",
        nargs="?",
        default="HEAD",
    )
    diff_parser.add_argument(
        "b_rev",
        help="New Git commit to compare (defaults to the current workspace)",
        nargs="?",
    )
    diff_parser.add_argument(
        "--json",
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
        "--md",
        help="Show tabulated output in the Markdown format (GFM).",
        action="store_true",
        dest="markdown",
        default=False,
    )
    diff_parser.add_argument(
        "--hide-missing",
        help="Hide missing cache file status.",
        action="store_true",
    )
    diff_parser.set_defaults(func=CmdDiff)
