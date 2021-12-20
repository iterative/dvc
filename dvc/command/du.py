import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBaseNoRepo, append_doc_link
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


def _human_readable_size(size):
    for unit in ["B", "K", "M", "G"]:
        if size < 1024.0 or unit == "G":
            break
        size /= 1024.0
    return "{0} {1}".format(round(size, 2), unit)


def _stringify(entries, human_readable=False):
    return [
        "{0} \t {1}".format(
            _human_readable_size(entry["size"])
            if human_readable
            else entry["size"],
            entry["path"],
        )
        for entry in entries
    ]


class CmdDU(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            entries, total_size = Repo.du(
                self.args.url,
                path=self.args.path,
                rev=self.args.rev,
                summarize=self.args.summarize,
                max_depth=self.args.max_depth,
                include_files=self.args.all,
            )
            if self.args.json:
                ui.write_json(entries)
            elif entries:
                entries_str = _stringify(
                    entries, human_readable=self.args.human_readable
                )
                ui.write("\n".join(entries_str))
                if self.args.total:
                    if self.args.human_readable:
                        ui.write(_human_readable_size(total_size))
                    else:
                        ui.write(total_size)
            return 0
        except DvcException:
            logger.exception(f"failed to summarize '{self.args.url}'")
            return 1


def add_parser(subparsers, parent_parser):
    DU_HELP = (
        "Summarize disk usage of the set of FILEs, "
        "recursively for directories "
        "tracked by DVC and by Git."
    )
    du_parser = subparsers.add_parser(
        "du",
        parents=[parent_parser],
        description=append_doc_link(DU_HELP, "du"),
        help=DU_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    du_parser.add_argument("url", help="Location of DVC repository")
    # using captial h (H) becuase small h (h) is used for help
    du_parser.add_argument(
        "-H",
        "--human-readable",
        action="store_true",
        help="print sizes in human readable format (e.g., 1K 234M 2G)",
    )
    du_parser.add_argument(
        "-d",
        "--max-depth",
        type=int,
        default=-1,
        nargs="?",
        help="""print the total for a directory (or file, with --all) only
              if it is N or fewer levels below the command line
              argument;  --max-depth=0 is the same as --summarize
            """,
        metavar="<integer>",
    )
    du_parser.add_argument(
        "-c",
        "--total",
        action="store_true",
        help="produce a grand total",
    )
    du_parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="write counts for all files, not just directories",
    )
    du_parser.add_argument(
        "-s",
        "--summarize",
        action="store_true",
        help="display only a total for each argument",
    )
    du_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    du_parser.add_argument(
        "--json",
        "--show-json",
        action="store_true",
        help="Show output in JSON format.",
    )
    du_parser.add_argument(
        "path",
        nargs="?",
        help="Path to directory within the repository to list outputs for",
    ).complete = completion.DIR
    du_parser.set_defaults(func=CmdDU)
