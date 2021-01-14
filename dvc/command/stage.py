import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers

logger = logging.getLogger(__name__)


class CmdStageAdd(CmdBase):
    @staticmethod
    def create(repo, force, **kwargs):
        from dvc.stage.utils import check_graphs, create_stage_from_cli

        stage = create_stage_from_cli(repo, **kwargs)
        check_graphs(repo, stage, force=force)
        return stage

    def run(self):
        stage = self.create(self.repo, self.args.force, **vars(self.args))
        stage.ignore_outs()
        stage.dump()

        return 0


def _add_common_args(parser):
    parser.add_argument(
        "-d",
        "--deps",
        action="append",
        default=[],
        help="Declare dependencies for reproducible cmd.",
        metavar="<path>",
    ).complete = completion.FILE
    parser.add_argument(
        "-o",
        "--outs",
        action="append",
        default=[],
        help="Declare output file or directory.",
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "-O",
        "--outs-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory "
        "(do not put into DVC cache).",
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "-p",
        "--params",
        action="append",
        default=[],
        help="Declare parameter to use as additional dependency.",
        metavar="[<filename>:]<params_list>",
    ).complete = completion.FILE
    parser.add_argument(
        "-m",
        "--metrics",
        action="append",
        default=[],
        help="Declare output metrics file.",
        metavar="<path>",
    )
    parser.add_argument(
        "-M",
        "--metrics-no-cache",
        action="append",
        default=[],
        help="Declare output metrics file (do not put into DVC cache).",
        metavar="<path>",
    )
    parser.add_argument(
        "--plots",
        action="append",
        default=[],
        help="Declare output plot file.",
        metavar="<path>",
    )
    parser.add_argument(
        "--plots-no-cache",
        action="append",
        default=[],
        help="Declare output plot file (do not put into DVC cache).",
        metavar="<path>",
    )
    parser.add_argument(
        "--live", help="Declare output as dvclive.", metavar="<path>",
    )
    parser.add_argument(
        "--live-no-summary",
        action="store_true",
        default=False,
        help="Signal dvclive logger to not dump latest metrics file.",
    )
    parser.add_argument(
        "--live-no-report",
        action="store_true",
        default=False,
        help="Signal dvclive logger to not produce training report.",
    )
    parser.add_argument(
        "-w",
        "--wdir",
        help="Directory within your repo to run your command in.",
        metavar="<path>",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Overwrite existing stage",
    )
    parser.add_argument(
        "--outs-persist",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro.",
        metavar="<filename>",
    )
    parser.add_argument(
        "--outs-persist-no-cache",
        action="append",
        default=[],
        help="Declare output file or directory that will not be "
        "removed upon repro (do not put into DVC cache).",
        metavar="<filename>",
    )
    parser.add_argument(
        "--checkpoints",
        action="append",
        default=[],
        help=argparse.SUPPRESS,
        metavar="<filename>",
    ).complete = completion.FILE
    parser.add_argument(
        "--always-changed",
        action="store_true",
        default=False,
        help="Always consider this DVC-file as changed.",
    )
    parser.add_argument(
        "--external",
        action="store_true",
        default=False,
        help="Allow outputs that are outside of the DVC repository.",
    )
    parser.add_argument(
        "--desc",
        type=str,
        metavar="<text>",
        help=(
            "User description of the stage (optional). "
            "This doesn't affect any DVC operations."
        ),
    )


def add_parser(subparsers, parent_parser):
    STAGES_HELP = "Commands to list and create stages."

    stage_parser = subparsers.add_parser(
        "stage",
        parents=[parent_parser],
        description=append_doc_link(STAGES_HELP, "stage"),
        help=STAGES_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    stage_subparsers = stage_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc stage CMD --help` to display command-specific help.",
    )

    fix_subparsers(stage_subparsers)

    STAGE_ADD_HELP = "Create stage"
    stage_add_parser = stage_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(STAGE_ADD_HELP, "stage/add"),
        help=STAGE_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    stage_add_parser.add_argument("name", help="Name of the stage to add")
    stage_add_parser.add_argument(
        "-c",
        "--command",
        action="append",
        default=[],
        dest="cmd",
        help="Command to execute.",
        required=True,
    )
    _add_common_args(stage_add_parser)
    stage_add_parser.set_defaults(func=CmdStageAdd)
