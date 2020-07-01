import argparse
import logging

import shtab

from dvc.command.base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)
FILE = shtab.FILE
DIR = shtab.DIRECTORY
DVC_FILE = {
    "bash": "_dvc_compgen_DVCFiles",
    "zsh": "_files -g '(*?.dvc|Dvcfile|dvc.yaml)'",
}
PREAMBLE = {
    "bash": """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -d -S '/' -- $1  # recurse into subdirs
  compgen -f -X '!*?.dvc' -- $1
  compgen -f -X '!*Dvcfile' -- $1
  compgen -f -X '!*dvc.yaml' -- $1
}
""",
    "zsh": "",
}


class CmdCompletion(CmdBaseNoRepo):
    def run(self):
        from dvc.cli import get_main_parser

        parser = get_main_parser()
        shell = self.args.shell
        script = shtab.complete(parser, shell=shell, preamble=PREAMBLE)
        print(script)
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
        choices=["bash", "zsh"],
    )
    completion_parser.set_defaults(func=CmdCompletion)
