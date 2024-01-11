from dvc.cli import formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdVersion(CmdBaseNoRepo):
    def run(self):
        from dvc.info import get_dvc_info
        from dvc.updater import notify_updates

        dvc_info = get_dvc_info()
        ui.write(dvc_info, force=True)

        notify_updates()
        return 0


def add_parser(subparsers, parent_parser):
    VERSION_HELP = "Display the DVC version and system/environment information."
    version_parser = subparsers.add_parser(
        "version",
        parents=[parent_parser],
        description=append_doc_link(VERSION_HELP, "version"),
        help=VERSION_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
        aliases=["doctor"],
    )
    version_parser.set_defaults(func=CmdVersion)
