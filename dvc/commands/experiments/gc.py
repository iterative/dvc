import argparse
import logging

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import InvalidArgumentError
from dvc.ui import ui

logger = logging.getLogger(__name__)


def _raise_error_if_all_disabled(**kwargs):
    if not any(kwargs.values()):
        raise InvalidArgumentError(
            "Either of `-w|--workspace`, `-a|--all-branches`, `-T|--all-tags` "
            "or `--all-commits` needs to be set."
        )


class CmdExperimentsGC(CmdBase):
    def run(self):
        _raise_error_if_all_disabled(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            workspace=self.args.workspace,
        )

        msg = "This will remove all experiments except those derived from "

        msg += "the workspace"
        if self.args.all_commits:
            msg += " and all git commits"
        elif self.args.all_branches and self.args.all_tags:
            msg += " and all git branches and tags"
        elif self.args.all_branches:
            msg += " and all git branches"
        elif self.args.all_tags:
            msg += " and all git tags"
        msg += " of the current repo."
        if self.args.queued:
            msg += " Run queued experiments will be preserved."
        else:
            msg += " Run queued experiments will be removed."

        logger.warning(msg)

        msg = "Are you sure you want to proceed?"
        if not self.args.force and not ui.confirm(msg):
            return 1

        removed = self.repo.experiments.gc(
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            all_commits=self.args.all_commits,
            workspace=self.args.workspace,
            queued=self.args.queued,
        )

        if removed:
            ui.write(
                f"Removed {removed} experiments.",
                "To remove unused cache files",
                "use 'dvc gc'.",
            )
        else:
            ui.write("No experiments to remove.")
        return 0


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_GC_HELP = "Garbage collect unneeded experiments."
    EXPERIMENTS_GC_DESCRIPTION = (
        "Removes all experiments which are not derived from the specified"
        "Git revisions."
    )
    experiments_gc_parser = experiments_subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_GC_DESCRIPTION, "exp/gc"),
        help=EXPERIMENTS_GC_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    experiments_gc_parser.add_argument(
        "-w",
        "--workspace",
        action="store_true",
        default=False,
        help="Keep experiments derived from the current workspace.",
    )
    experiments_gc_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Keep experiments derived from the tips of all Git branches.",
    )
    experiments_gc_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Keep experiments derived from all Git tags.",
    )
    experiments_gc_parser.add_argument(
        "-A",
        "--all-commits",
        action="store_true",
        default=False,
        help="Keep experiments derived from all Git commits.",
    )
    experiments_gc_parser.add_argument(
        "--queued",
        action="store_true",
        default=False,
        help=(
            "Keep queued experiments (experiments run queue will be cleared "
            "by default)."
        ),
    )
    experiments_gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection - automatically agree to all prompts.",
    )
    experiments_gc_parser.set_defaults(func=CmdExperimentsGC)
