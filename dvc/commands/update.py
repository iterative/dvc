import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


class CmdUpdate(CmdBase):
    def run(self):
        ret = 0
        try:
            self.repo.update(
                targets=self.args.targets,
                rev=self.args.rev,
                recursive=self.args.recursive,
                to_remote=self.args.to_remote,
                no_download=self.args.no_download,
                remote=self.args.remote,
                jobs=self.args.jobs,
            )
        except DvcException:
            logger.exception("failed update data")
            ret = 1
        return ret


def add_parser(subparsers, parent_parser):
    UPDATE_HELP = (
        "Update data artifact imported (via dvc import or dvc import-url) "
        "from an external DVC repository or URL, or update data from a "
        "DVC worktree remote."
    )
    update_parser = subparsers.add_parser(
        "update",
        parents=[parent_parser],
        description=append_doc_link(UPDATE_HELP, "update"),
        help=UPDATE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    update_parser.add_argument(
        "targets", nargs="+", help=".dvc files to update."
    ).complete = completion.DVC_FILE
    update_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    update_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Update all stages in the specified directory.",
    )
    update_parser.add_argument(
        "--no-download",
        action="store_true",
        default=False,
        help="Update .dvc file git revision/hash value(s)"
        " but do not download the file(s).",
    )
    update_parser.add_argument(
        "--to-remote",
        action="store_true",
        default=False,
        help="Update data directly on the remote",
    )
    update_parser.add_argument(
        "-r",
        "--remote",
        help="Remote storage to perform updates to",
        metavar="<name>",
    )
    update_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        help=(
            "Number of jobs to run simultaneously. "
            "The default value is 4 * cpu_count(). "
        ),
        metavar="<number>",
    )
    update_parser.set_defaults(func=CmdUpdate)
