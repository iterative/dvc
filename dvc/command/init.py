import argparse
import logging

from dvc.command.base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)


class CmdInit(CmdBaseNoRepo):
    def run(self):
        from dvc.exceptions import InitError
        from dvc.repo import Repo

        try:
            self.repo = Repo.init(
                ".",
                no_scm=self.args.no_scm,
                force=self.args.force,
                subdir=self.args.subdir,
            )
            self.config = self.repo.config
        except InitError:
            logger.exception("failed to initiate DVC")
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    """Setup parser for `dvc init`."""
    INIT_HELP = "Initialize DVC in the current directory."
    INIT_DESCRIPTION = (
        "Initialize DVC in the current directory. Expects directory\n"
        "to be a Git repository unless --no-scm option is specified."
    )

    init_parser = subparsers.add_parser(
        "init",
        parents=[parent_parser],
        description=append_doc_link(INIT_DESCRIPTION, "init"),
        help=INIT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    init_parser.add_argument(
        "--no-scm",
        action="store_true",
        default=False,
        help="Initiate DVC in directory that is "
        "not tracked by any SCM tool (e.g. Git).",
    )
    init_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help=(
            "Overwrite existing '.dvc/' directory. "
            "This operation removes local cache."
        ),
    )
    init_parser.add_argument(
        "--subdir",
        action="store_true",
        default=False,
        help=(
            "Necessary for running this command inside a subdirectory of a "
            "parent SCM repository."
        ),
    )
    init_parser.set_defaults(func=CmdInit)
