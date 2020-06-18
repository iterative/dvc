import argparse
import logging
import os

import shtab

from dvc.command import choices
from dvc.command.base import CmdBase, append_doc_link

logger = logging.getLogger(__name__)
CHOICE_FUNCTIONS = {
    "bash": {"DVCFile": "_dvc_compgen_DVCFiles"},
    "zsh": {"DVCFile": "_files -g '(*.dvc|Dvcfile)'"},
}
PREAMBLE = {
    "bash": """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -f -X '!*?.dvc' -- $1  # DVC-files
  compgen -d -S '/' -- $1  # recurse into subdirs
}
""",
    "zsh": "",
}


class CmdCompletion(CmdBase):
    def run(self):
        from dvc.cli import get_main_parser

        parser = get_main_parser()
        script = shtab.complete(
            parser,
            shell=self.args.shell,
            preamble=PREAMBLE[self.args.shell],
            choice_functions=CHOICE_FUNCTIONS[self.args.shell],
        )

        if self.args.dir == "-":
            logger.debug("Writing tab completion to stdout")
            print(script)
        else:
            logger.info(
                f"Writing tab completion to {self.args.dir}/{self.args.file}"
            )
            with open(os.path.join(self.args.dir, self.args.file), "w") as fd:
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
        "-f", "--file", help="File name for output.", default="dvc",
    )
    completion_parser.add_argument(
        "dir",
        help="Output directory for completion script. Use - for stdout.",
        default="-",
        nargs="*",
        choices=choices.Required.DIR,
    )
    completion_parser.set_defaults(func=CmdCompletion)
