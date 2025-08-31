import logging

from dvc.cli import completion, formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdDU(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo
        from dvc.utils.humanize import naturalsize

        entries = Repo.du(
            self.args.url,
            self.args.path,
            rev=self.args.rev,
            summarize=self.args.summarize,
            config=self.args.config,
            remote=self.args.remote,
            remote_config=self.args.remote_config,
        )
        ui.table([(naturalsize(size), path) for path, size in entries])
        return 0


def add_parser(subparsers, parent_parser):
    DU_HELP = "Show disk usage."
    du_parser = subparsers.add_parser(
        "du",
        parents=[parent_parser],
        description=append_doc_link(DU_HELP, "du"),
        help=DU_HELP,
        formatter_class=formatter.RawTextHelpFormatter,
    )
    du_parser.add_argument("url", help="Location of DVC repository")
    du_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    du_parser.add_argument(
        "-s",
        "--summarize",
        action="store_true",
        help="Show total disk usage.",
    )
    du_parser.add_argument(
        "--config",
        type=str,
        help=(
            "Path to a config file that will be merged with the config "
            "in the target repository."
        ),
    )
    du_parser.add_argument(
        "--remote",
        type=str,
        help="Remote name to set as a default in the target repository.",
    ).complete = completion.REMOTE
    du_parser.add_argument(
        "--remote-config",
        type=str,
        nargs="*",
        action=DictAction,
        help=(
            "Remote config options to merge with a remote's config (default or one "
            "specified by '--remote') in the target repository."
        ),
    )
    du_parser.add_argument(
        "path",
        nargs="?",
        help="Path to directory within the repository",
    ).complete = completion.DIR
    du_parser.set_defaults(func=CmdDU)
