import argparse
import logging

import shtab

from dvc.command import choices
from dvc.command.base import CmdBase, append_doc_link

logger = logging.getLogger(__name__)
choice_functions = {"DVCFile": "_dvc_compgen_DVCFiles"}
preamble = """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -f -X '!*?.dvc' -- $1  # DVC-files
  compgen -d -S '/' -- $1  # recurse into subdirs
}
"""


class CmdCompletion(CmdBase):
    def run(self):
        from dvc.cli import get_main_parser

        parser = get_main_parser()
        script = shtab.complete(
            parser,
            shell="bash",
            preamble=preamble,
            choice_functions=choice_functions,
        )

        if self.args.output == "-":
            logger.debug("Writing bash completion to stdout")
            print(script)
        else:
            logger.info(f"Writing bash completion to {self.args.output}")
            with open(self.args.output, "w") as fd:
                print(script, file=fd)
        return 0


def add_parser(subparsers, parent_parser):
    COMPLETION_HELP = "Enable shell tab completion."
    COMPLETION_DESCRIPTION = (
        "Automatically generates shell tab completion script."
    )
    completion_parser = subparsers.add_parser(
        "completion",
        parents=[parent_parser],
        description=append_doc_link(COMPLETION_DESCRIPTION, "completion"),
        help=COMPLETION_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    completion_parser.add_argument(
        "-o",
        "--output",
        help="Output filename for completion script. Use - for stdout.",
        metavar="<path>",
        default="-",
        choices=choices.Required.DIR,
    )
    completion_parser.set_defaults(func=CmdCompletion)
