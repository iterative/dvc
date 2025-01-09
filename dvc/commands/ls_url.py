from dvc.cli import formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.log import logger

from .ls import show_entries, show_tree

logger = logger.getChild(__name__)


class CmdListUrl(CmdBaseNoRepo):
    def _show_tree(self, config):
        from dvc.fs import parse_external_url
        from dvc.repo.ls import _ls_tree

        fs, fs_path = parse_external_url(
            self.args.url, fs_config=self.args.fs_config, config=config
        )
        entries = _ls_tree(fs, fs_path, maxdepth=self.args.level)
        show_tree(entries, with_color=True, with_size=self.args.size)
        return 0

    def _show_list(self, config):
        from dvc.repo import Repo

        entries = Repo.ls_url(
            self.args.url,
            recursive=self.args.recursive,
            maxdepth=self.args.level,
            fs_config=self.args.fs_config,
            config=config,
        )
        if entries:
            show_entries(entries, with_color=True, with_size=self.args.size)
        return 0

    def run(self):
        from dvc.config import Config

        config = Config.from_cwd()
        if self.args.tree:
            return self._show_tree(config=config)
        return self._show_list(config=config)


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
    lsurl_parser.add_argument(
        "-T",
        "--tree",
        action="store_true",
        help="Recurse into directories as a tree.",
    )
    lsurl_parser.add_argument(
        "-L",
        "--level",
        metavar="depth",
        type=int,
        help="Limit the depth of recursion.",
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
