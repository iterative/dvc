import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

logger = logging.getLogger(__name__)


class CmdExperimentsList(CmdBase):
    def run(self):
        names_only = self.args.names_only
        exps = self.repo.experiments.ls(
            rev=self.args.rev,
            git_remote=self.args.git_remote,
            all_=self.args.all,
        )
        for baseline in exps:
            tag = self.repo.scm.describe(baseline)
            if not tag:
                branch = self.repo.scm.describe(baseline, base="refs/heads")
                if branch:
                    tag = branch.split("/")[-1]
            name = tag if tag else baseline[:7]
            if not names_only:
                print(f"{name}:")
            for exp_name in exps[baseline]:
                indent = "" if names_only else "\t"
                print(f"{indent}{exp_name}")

        return 0


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_LIST_HELP = "List local and remote experiments."
    experiments_list_parser = experiments_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_LIST_HELP, "exp/list"),
        help=EXPERIMENTS_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_list_parser.add_argument(
        "--rev",
        type=str,
        default=None,
        help=(
            "List experiments derived from the specified revision. "
            "Defaults to HEAD if neither `--rev` nor `--all` are specified."
        ),
        metavar="<rev>",
    )
    experiments_list_parser.add_argument(
        "--all", action="store_true", help="List all experiments."
    )
    experiments_list_parser.add_argument(
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
        metavar="[<git_remote>]",
    )
    experiments_list_parser.set_defaults(func=CmdExperimentsList)
