from dvc.cli import formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.log import logger

from .ls import show_entries

logger = logger.getChild(__name__)


class CmdListUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.config import Config
        from dvc.repo import Repo

        entries = Repo.ls_url(
            self.args.url,
            recursive=self.args.recursive,
            fs_config=self.args.fs_config,
            config=Config.from_cwd(),
        )
        if entries:
            show_entries(entries, with_color=True, with_size=self.args.size)
        return 0


def add_parser(subparsers, parent_parser):
    LS_HELP = "List directory contents from URL."
    lsurl_parser = subparsers.add_parser(
        "list-url",
        aliases=["ls-url"],
        parents=[parent_parser],
        description=append_doc_link(LS_HELP, "list-url"),
        help=LS_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    lsurl_parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    lsurl_parser.add_argument(
        "-R", "--recursive", action="store_true", help="Recursively list files."
    )
    lsurl_parser.add_argument("--size", action="store_true", help="Show sizes.")
    lsurl_parser.add_argument(
        "--fs-config",
        type=str,
        nargs="*",
        action=DictAction,
        help="Config options for the target url.",
    )
    lsurl_parser.set_defaults(func=CmdListUrl)
