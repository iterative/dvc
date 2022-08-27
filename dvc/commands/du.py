import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdDu(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            entries = Repo.ls(
                self.args.url,
                self.args.path,
                rev=self.args.rev,
                recursive=True,
                dvc_only=self.args.dvc_only,
                dvc_as_dir=False,
                return_sizes=True,
            )
            sizes = [e["size"] for e in entries]
            if None in sizes:
                bad_paths = [e["path"] for e in entries if e["size"] is None]
                logger.exception(
                    "failed to evaluate disk-usage for '%s'",
                    ", ".join(bad_paths),
                )
                return 1
            target = "%s %s" % (self.args.url, self.args.path or "")
            ui.write("%d    %s" % (sum(sizes), target))
            return 0
        except DvcException:
            logger.exception(
                "failed to evaluate disk-usage for '%s'", self.args.url
            )
            return 1


def add_parser(subparsers, parent_parser):
    DU_HELP = (
        "Display disk-usage of repository contents, including files"
        " and directories tracked by DVC and by Git."
    )
    list_parser = subparsers.add_parser(
        "du",
        parents=[parent_parser],
        description=append_doc_link(DU_HELP, "du"),
        help=DU_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    list_parser.add_argument("url", help="Location of DVC repository to list")
    list_parser.add_argument(
        "--dvc-only", action="store_true", help="Include only DVC outputs."
    )
    list_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    list_parser.add_argument(
        "path",
        nargs="?",
        help="Path to directory within the repository to list outputs for",
    ).complete = completion.DIR
    # TODO: Add option for human-readable file-size output (i.e. KB/MB/GB).
    list_parser.set_defaults(func=CmdDu)
