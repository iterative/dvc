import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.commands.ls.ls_colors import LsColors
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


def _prettify(entries, with_color=False):
    if with_color:
        ls_colors = LsColors()
        fmt = ls_colors.format
    else:

        def fmt(entry):
            return entry["path"]

    return [fmt(entry) for entry in entries]


class CmdList(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            entries = Repo.ls(
                self.args.url,
                self.args.path,
                rev=self.args.rev,
                recursive=self.args.recursive,
                dvc_only=self.args.dvc_only,
                config=self.args.config,
                remote=self.args.remote,
                remote_config=self.args.remote_config,
            )
            if self.args.json:
                ui.write_json(entries)
            elif entries:
                entries = _prettify(entries, with_color=True)
                ui.write("\n".join(entries))
            return 0
        except DvcException:
            logger.exception("failed to list '%s'", self.args.url)
            return 1


def add_parser(subparsers, parent_parser):
    LIST_HELP = (
        "List repository contents, including files"
        " and directories tracked by DVC and by Git."
    )
    list_parser = subparsers.add_parser(
        "list",
        aliases=["ls"],
        parents=[parent_parser],
        description=append_doc_link(LIST_HELP, "list"),
        help=LIST_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    list_parser.add_argument("url", help="Location of DVC repository to list")
    list_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        help="Recursively list files.",
    )
    list_parser.add_argument(
        "--dvc-only", action="store_true", help="Show only DVC outputs."
    )
    list_parser.add_argument(
        "--json",
        action="store_true",
        help="Show output in JSON format.",
    )
    list_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    list_parser.add_argument(
        "--config",
        type=str,
        help=(
            "Path to a config file that will be merged with the config "
            "in the target repository."
        ),
    )
    list_parser.add_argument(
        "--remote",
        type=str,
        help="Remote name to set as a default in the target repository.",
    )
    list_parser.add_argument(
        "--remote-config",
        type=str,
        nargs="*",
        action=DictAction,
        help=(
            "Remote config options to merge with a remote's config (default or one "
            "specified by '--remote') in the target repository."
        ),
    )
    list_parser.add_argument(
        "path",
        nargs="?",
        help="Path to directory within the repository to list outputs for",
    ).complete = completion.DIR
    list_parser.set_defaults(func=CmdList)
