from dvc.cli import formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.utils import relpath

logger = logger.getChild(__name__)


class CmdRoot(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo
        from dvc.ui import ui

        ui.write(relpath(Repo.find_root()))
        return 0


def add_parser(subparsers, parent_parser):
    ROOT_HELP = "Return the relative path to the root of the DVC project."
    root_parser = subparsers.add_parser(
        "root",
        parents=[parent_parser],
        description=append_doc_link(ROOT_HELP, "root"),
        help=ROOT_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    root_parser.set_defaults(func=CmdRoot)
