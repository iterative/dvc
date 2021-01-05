import argparse
import logging

import shtab

from dvc.command.base import CmdBaseNoRepo, append_doc_link

logger = logging.getLogger(__name__)
FILE = shtab.FILE
DIR = shtab.DIRECTORY

PREAMBLE = {
    "bash": """
# $1=COMP_WORDS[1]
_dvc_compgen_DVCFiles() {
  compgen -d -S '/' -- $1  # recurse into subdirs
  compgen -f -X '!*?.dvc' -- $1
  compgen -f -X '!*Dvcfile' -- $1
  compgen -f -X '!*dvc.yaml' -- $1
}

_dvc_compgen_stages() {
    local _dvc_stages=($(dvc stages -sq))
    compgen -W "${_dvc_stages[*]}" -- $1
}

_dvc_compgen_stages_and_files() {
    _dvc_compgen_DVCFiles $1
    _dvc_compgen_stages $1
}

    """,
    "zsh": """
_dvc_compadd_DVCFiles() {
    _files -g '(*?.dvc|Dvcfile|dvc.yaml)'
}

_dvc_compadd_stages() {
    # this will also show up the description of the stages
    _describe 'command' "($(dvc stages -sq --with-desc))"
}

_dvc_compadd_stages_and_files() {
    _dvc_compadd_DVCFiles
    _dvc_compadd_stages
}
    """,
}

DVC_FILE = {
    "bash": "_dvc_compgen_DVCFiles",
    "zsh": "_dvc_compadd_DVCFiles",
}

STAGE = {
    "bash": "_dvc_compgen_stages",
    "zsh": "_dvc_compadd_stages",
}

DVCFILES_AND_STAGE = {
    "bash": "_dvc_compgen_stages_and_files",
    "zsh": "_dvc_compadd_stages_and_files",
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
