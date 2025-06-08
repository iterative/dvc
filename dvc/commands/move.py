from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdMove(CmdBase):
    def run(self):
        try:
            self.repo.move(self.args.src, self.args.dst)
        except DvcException:
            msg = f"failed to move '{self.args.src}' -> '{self.args.dst}'"
            logger.exception(msg)
            return 1
        return 0


def add_parser(subparsers, parent_parser):
    MOVE_HELP = "Rename or move a DVC controlled data file or a directory."
    MOVE_DESCRIPTION = (
        "Rename or move a DVC controlled data file or a directory.\n"
        "It renames and modifies the corresponding .dvc file to reflect the"
        " changes."
    )

    move_parser = subparsers.add_parser(
        "move",
        aliases=["mv"],
        parents=[parent_parser],
        description=append_doc_link(MOVE_DESCRIPTION, "move"),
        help=MOVE_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    move_parser.add_argument(
        "src", help="Source path to a data file or directory."
    ).complete = completion.FILE
    move_parser.add_argument("dst", help="Destination path.").complete = completion.FILE
    move_parser.set_defaults(func=CmdMove)
