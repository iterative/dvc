import argparse
import logging

from dvc.command import completion
from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdVdirBase(CmdBase):
    UNINITIALIZED = True


class CmdVdirPull(CmdVdirBase):
    def run(self):
        try:
            self.repo.vdir.pull(self.args.targets)
        except DvcException:
            logger.exception(
                "failed to pull virtual directory from: {}".format(
                    self.args.targets
                )
            )
            return 1

        return 0


class CmdVdirAdd(CmdVdirBase):
    def run(self):
        try:
            self.repo.vdir.add(self.args.paths)
        except DvcException:
            logger.exception(
                "failed to add into the virtual directory: {}".format(
                    self.args.paths
                )
            )
            return 1

        return 0


def add_parser(subparsers, parent_parser):
    VDIR_HELP = "Commands to transact with virtual directories."

    vdir_parser = subparsers.add_parser(
        "vdir",
        parents=[parent_parser],
        description=append_doc_link(VDIR_HELP, "vdir"),
        help=VDIR_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    vdir_subparsers = vdir_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc vdir CMD --help` to display command-specific help.",
    )

    fix_subparsers(vdir_subparsers)

    VDIR_PULL_HELP = (
        "Download the list of tracked files or directories from remote storage"
        " and treat them as virtual directories in the local workspace."
    )
    vdir_pull_parser = vdir_subparsers.add_parser(
        "pull",
        parents=[parent_parser],
        description=append_doc_link(VDIR_PULL_HELP, "vdir/pull"),
        help=VDIR_PULL_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    vdir_pull_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, .dvc "
            "files, or stage names."
        ),
        metavar="<targets>",
    ).complete = completion.FILE
    vdir_pull_parser.set_defaults(func=CmdVdirPull)

    VDIR_ADD_HELP = "Adds files or directories into the virtual directory."

    vdir_add_parser = vdir_subparsers.add_parser(
        "add",
        parents=[parent_parser],
        description=append_doc_link(VDIR_ADD_HELP, "vdir/add"),
        help=VDIR_ADD_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    vdir_add_parser.add_argument(
        "paths",
        nargs="*",
        help=(
            "Local paths of the files and directories that is added to the "
            "virtual directory."
        ),
        metavar="<paths>",
    ).complete = completion.FILE
    vdir_add_parser.set_defaults(func=CmdVdirAdd)
