import argparse
import logging

from io import StringIO

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link
from dvc.ui import ui


logger = logging.getLogger(__name__)
NOTE_ACTIONS = ["find", "list", "remove", "set"]


class CmdNote(CmdBase):
    def run(self):
        from dvc.exceptions import DvcException
        try:
            results = self.repo.note(self.args.action, self.args.targets,
                                     self.args.key, self.args.value)
            if self.args.action == "find":
                ui.write(self._format_find(results))
            elif self.args.action == "list":
                ui.write(self._format_list(results))
            return 0
        except DvcException:
            logger.exception("")
            return 1

    def _format_find(self, results):
        buffer = StringIO()
        if len(results) == 1:
            _, _, value = results[0]
            return value
        else:
            for (target, key, value) in results:
                print(f"{target}: {value}", file=buffer)
        return buffer.getvalue()[:-1] # to remove last newline

    def _format_list(self, results):
        buffer = StringIO()
        if len(results) == 1:
            _, keys = results[0]
            for key in keys:
                print(key, file=buffer)
        else:
            for (target, keys) in results:
                print(target, file=buffer)
                for key in sorted(keys):
                    print(f"- {key}", file=buffer)
        return buffer.getvalue()[:-1] # to remove last newline


def add_parser(subparsers, parent_parser):
    NOTE_HELP = "Add, remove, view, and search notes."
    NOTE_DESCRIPTION = "Manage notes associated with DVC\n"
    note_parser = subparsers.add_parser(
        "note",
        parents=[parent_parser],
        description=append_doc_link(NOTE_DESCRIPTION, "note"),
        help=NOTE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    note_parser.add_argument(
        "action",
        type=str,
        choices=NOTE_ACTIONS,
        default="list",
        help="Action to take.",
    )
    note_parser.add_argument(
        "-K",
        "--key",
        type=str,
        help="Key.",
    )
    note_parser.add_argument(
        "-V",
        "--value",
        type=str,
        help="Value.",
    )
    note_parser.add_argument(
        "targets",
        nargs="+",
        help="File(s)."
    ).complete = completion.FILE
    note_parser.set_defaults(func=CmdNote)
