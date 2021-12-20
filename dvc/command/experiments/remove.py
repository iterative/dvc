import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link

logger = logging.getLogger(__name__)


class CmdExperimentsRemove(CmdBase):
    def run(self):

        self.repo.experiments.remove(
            exp_names=self.args.experiment,
            queue=self.args.queue,
            clear_all=self.args.all,
            remote=self.args.git_remote,
        )

        return 0


def add_parser(experiments_subparsers, parent_parser):

    EXPERIMENTS_REMOVE_HELP = "Remove experiments."
    experiments_remove_parser = experiments_subparsers.add_parser(
        "remove",
        parents=[parent_parser],
        description=append_doc_link(EXPERIMENTS_REMOVE_HELP, "exp/remove"),
        help=EXPERIMENTS_REMOVE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    remove_group = experiments_remove_parser.add_mutually_exclusive_group()
    remove_group.add_argument(
        "--queue", action="store_true", help="Remove all queued experiments."
    )
    remove_group.add_argument(
        "-A",
        "--all",
        action="store_true",
        help="Remove all committed experiments.",
    )
    remove_group.add_argument(
        "-g",
        "--git-remote",
        metavar="<git_remote>",
        help="Name or URL of the Git remote to remove the experiment from",
    )
    experiments_remove_parser.add_argument(
        "experiment",
        nargs="*",
        help="Experiments to remove.",
        metavar="<experiment>",
    )
    experiments_remove_parser.set_defaults(func=CmdExperimentsRemove)
