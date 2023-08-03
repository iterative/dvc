import argparse
import logging

from dvc.cli.command import CmdBaseNoRepo
from dvc.cli.completion import PREAMBLE
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


SUPPORTED_SHELLS = ["bash", "zsh"]


class CmdCompletion(CmdBaseNoRepo):
    def run(self):
        import shtab

        shell = self.args.shell
        parser = self.args.parser
        script = shtab.complete(parser, shell=shell, preamble=PREAMBLE)  # nosec B604
        ui.write(script, force=True)
        return 0


def add_parser(subparsers, parent_parser):
    COMPLETION_HELP = "Generate shell tab completion."
    COMPLETION_DESCRIPTION = "Prints out shell tab completion scripts."
    completion_parser = subparsers.add_parser(
        "completion",
        parents=[parent_parser],
        description=append_doc_link(COMPLETION_DESCRIPTION, "completion"),
        help=COMPLETION_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    completion_parser.add_argument(
        "-s",
        "--shell",
        help="Shell syntax for completions.",
        default="bash",
        choices=SUPPORTED_SHELLS,
    )
    completion_parser.set_defaults(func=CmdCompletion)
