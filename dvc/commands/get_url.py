from dvc.cli import completion, formatter
from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.utils import DictAction, append_doc_link
from dvc.exceptions import DvcException
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdGetUrl(CmdBaseNoRepo):
    def run(self):
        from dvc.config import Config
        from dvc.repo import Repo

        try:
            Repo.get_url(
                self.args.url,
                out=self.args.out,
                jobs=self.args.jobs,
                force=self.args.force,
                fs_config=self.args.fs_config,
                config=Config.from_cwd(),
            )
            return 0
        except DvcException:
            logger.exception("failed to get '%s'", self.args.url)
            return 1


def add_parser(subparsers, parent_parser):
    GET_HELP = "Download or copy files from URL."
    get_parser = subparsers.add_parser(
        "get-url",
        parents=[parent_parser],
        description=append_doc_link(GET_HELP, "get-url"),
        help=GET_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    get_parser.add_argument(
        "url", help="See `dvc import-url -h` for full list of supported URLs."
    )
    get_parser.add_argument(
        "out", nargs="?", help="Destination path to put data to."
    ).complete = completion.DIR
    get_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    get_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Override local file or folder if exists.",
    )
    get_parser.add_argument(
        "--fs-config",
        type=str,
        nargs="*",
        action=DictAction,
        help="Config options for the target url.",
    )
    get_parser.set_defaults(func=CmdGetUrl)
