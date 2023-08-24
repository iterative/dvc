import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdExperimentsList(CmdBase):
    def run(self):
        name_only = self.args.name_only
        sha_only = self.args.sha_only
        git_remote = self.args.git_remote
        if sha_only and git_remote:
            raise InvalidArgumentError("--sha-only not supported with git_remote.")
        exps = self.repo.experiments.ls(
            all_commits=self.args.all_commits,
            rev=self.args.rev,
            num=self.args.num,
            git_remote=git_remote,
        )

        from dvc.repo.experiments.utils import describe
        from dvc.scm import Git

        if name_only or sha_only:
            names = {}
        else:
            assert isinstance(self.repo.scm, Git)
            names = describe(
                self.repo.scm,
                (baseline for baseline in exps),
                logger=logger,
            )

        for baseline in exps:
            if not (name_only or sha_only):
                name = names.get(baseline) or baseline[:7]
                ui.write(f"{name}:")
            for exp_name, rev in exps[baseline]:
                if name_only:
                    ui.write(exp_name)
                elif sha_only:
                    ui.write(rev)
                elif rev:
                    ui.write(f"\t{rev[:7]} [{exp_name}]")
                else:
                    ui.write(f"\t{exp_name}")

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
    display_group = experiments_list_parser.add_mutually_exclusive_group()
    display_group.add_argument(
        "--name-only",
        "--names-only",
        action="store_true",
        help="Only output experiment names (without SHAs or parent commits).",
    )
    display_group.add_argument(
        "--sha-only",
        "--shas-only",
        action="store_true",
        help="Only output experiment commit SHAs (without names or parent commits).",
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
