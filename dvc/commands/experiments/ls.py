import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link

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
        tags = self.repo.scm.describe(exps)
        remained = {baseline for baseline, tag in tags.items() if tag is None}
        base = "refs/heads/"
        ref_heads = self.repo.scm.describe(remained, base=base)

        for baseline in exps:
            name = (
                tags[baseline]
                or ref_heads[baseline][len(base) :]
                or baseline[:7]
            )
            if not name_only:
                print(f"{name}:")
            for exp_name in exps[baseline]:
                indent = "" if name_only else "\t"
                print(f"{indent}{exp_name}")

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
