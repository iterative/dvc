import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdInstall(CmdBase):
    def run(self):
        try:
            self.repo.install(self.args.use_pre_commit_tool)
        except DvcException:
            logger.exception("failed to install DVC Git hooks")
            return 1
        return 0


def add_parser(subparsers, add_common_args):
    INSTALL_HELP = "Install DVC git hooks into the repository."
    install_parser = subparsers.add_parser(
        "install",
        description=append_doc_link(INSTALL_HELP, "install"),
        add_help=False,
        help=INSTALL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    install_parser.add_argument(
        "--use-pre-commit-tool",
        action="store_true",
        default=False,
        help="Install DVC hooks using pre-commit "
        "(https://pre-commit.com) if it is installed.",
    )
    add_common_args(install_parser, func=CmdInstall)
