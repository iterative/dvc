import argparse
import logging
import os

import dvc.prompt as prompt
from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase


logger = logging.getLogger(__name__)


class CmdGC(CmdBase):
    def run(self):
        msg = (
            "This will remove all cache except items used in the working tree"
            "{tags}{history}{branches}{history}"
        ).format(
            tags="" if self.args.remove_all_tags else "and all git tags",
            branches=""
            if self.args.remove_all_branches
            else "and all git branches",
            history=""
            if self.args.remove_all_history
            else "and their history",
        )

        if self.args.repos:
            msg += " of the current and the following repos:"

            for repo_path in self.args.repos:
                msg += "\n  - %s" % os.path.abspath(repo_path)
        else:
            msg += " of the current repo."

        logger.warning(msg)

        msg = "Are you sure you want to proceed?"
        if not self.args.force and not prompt.confirm(msg):
            return 1

        self.repo.gc(
            remove_all_tags=self.args.remove_all_tags,
            remove_all_branches=self.args.remove_all_branches,
            remove_all_history=self.args.remove_all_history,
            cloud=self.args.cloud,
            remote=self.args.remote,
            force=self.args.force,
            jobs=self.args.jobs,
            repos=self.args.repos,
        )
        return 0


def add_parser(subparsers, parent_parser):
    GC_HELP = "Collect unused data from DVC cache or a remote storage."
    GC_DESCRIPTION = (
        "Deletes all files in the cache or a remote which are not in\n"
        "use by the specified git references (defaults to just HEAD)."
    )
    gc_parser = subparsers.add_parser(
        "gc",
        parents=[parent_parser],
        description=append_doc_link(GC_DESCRIPTION, "gc"),
        help=GC_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gc_parser.add_argument(
        "-c",
        "--cloud",
        action="store_true",
        default=False,
        help="Collect garbage in remote repository.",
    )
    gc_parser.add_argument(
        "-r", "--remote", help="Remote storage to collect garbage in."
    )
    gc_parser.add_argument(
        "--remove-all-tags",
        action="store_true",
        default=False,
        help="Remove cache for all git tags.",
    )
    gc_parser.add_argument(
        "--remove-all-branches",
        action="store_true",
        default=False,
        help="Remove cache for all git branches.",
    )
    gc_parser.add_argument(
        "--remove-all-history",
        action="store_true",
        default=False,
        help="Remove cache for all history of all branches and tags.",
    )
    gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection - automatically agree to all prompts.",
    )
    gc_parser.add_argument(
        "-j", "--jobs", type=int, help="Number of jobs to run simultaneously."
    )
    gc_parser.add_argument(
        "-p",
        "--projects",
        dest="repos",
        type=str,
        nargs="*",
        help="Keep data files required by these projects "
        "in addition to the current one. "
        "Useful if you share a single cache across repos.",
    )
    gc_parser.set_defaults(func=CmdGC)
