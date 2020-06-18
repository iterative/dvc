import argparse
import logging
import os

import shtab

from dvc.command.base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)
DEFAULT_FILENAME = {"bash": "dvc", "zsh": "_dvc"}
CHOICE_FUNCTIONS = {
    "bash": {"DVCFile": "_dvc_compgen_DVCFiles"},
    "zsh": {"DVCFile": "_files -g '(*?.dvc|Dvcfile|dvc.yaml)'"},
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


class choices:
    class Optional(shtab.Optional):
        DVC_FILE = [shtab.Choice("DVCFile", required=False)]

    class Required(shtab.Required):
        DVC_FILE = [shtab.Choice("DVCFile", required=True)]


class CmdCompletion(CmdBaseNoRepo):
    def run(self):
        from dvc.cli import get_main_parser

        parser = get_main_parser()
        shell = self.args.shell
        script = shtab.complete(
            parser,
            shell=shell,
            preamble=PREAMBLE[shell],
            choice_functions=CHOICE_FUNCTIONS[shell],
        )

        if self.args.dir == "-":
            logger.debug("Writing tab completion to stdout")
            print(script)
        else:
            fname = os.path.join(
                self.args.dir, self.args.file or DEFAULT_FILENAME[shell],
            )
            logger.info(f"Writing tab completion to {fname}")
            with open(fname, "w") as fd:
                print(script, file=fd)
        return 0


def add_parser(subparsers, parent_parser):
    COMPLETION_HELP = "Install shell tab completion."
    COMPLETION_DESCRIPTION = (
        "Automatically generates shell tab completion scripts."
    )
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
    completion_parser.add_argument(
        "-f",
        "--file",
        help=(
            "File name for output. Defaults depending on --shell:"
            f" {DEFAULT_FILENAME}."
        ),
    )
    completion_parser.add_argument(
        "dir",
        help=(
            "Output directory for completion script."
            " Defaults to '-' for stdout (ignoring --file)."
        ),
        default="-",
        nargs="?",
        choices=choices.Required.DIR,
    )
    completion_parser.set_defaults(func=CmdCompletion)
