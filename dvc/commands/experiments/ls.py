import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsList(CmdBase):
    def run(self):
        name_only = self.args.name_only
        exps = self.repo.experiments.ls(
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            git_remote=self.args.git_remote,
        )

        for baseline in exps:
            if not name_only:
                ui.write(f"{baseline}:")
            for exp_name in exps[baseline]:
                indent = "" if name_only else "\t"
                ui.write(f"{indent}{exp_name}")

        return 0


def add_parser(experiments_subparsers, parent_parser):
    from . import add_rev_selection_flags

    EXPERIMENTS_LIST_HELP = "List local and remote experiments."
    experiments_list_parser = experiments_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_LIST_HELP, "exp/list"),
        help=EXPERIMENTS_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    add_rev_selection_flags(experiments_list_parser, "List")
    experiments_list_parser.add_argument(
        "--name-only",
        "--names-only",
        action="store_true",
        help="Only output experiment names (without parent commits).",
    )
    experiments_list_parser.add_argument(
        "git_remote",
        nargs="?",
        default=None,
        help=(
            "Optional Git remote name or Git URL. "
            "If provided, experiments from the specified Git repository "
            " will be listed instead of local ones."
        ),
        metavar="<git_remote>",
    )
    experiments_list_parser.set_defaults(func=CmdExperimentsList)
