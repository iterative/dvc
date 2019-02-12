from __future__ import unicode_literals

import os
import dvc.prompt as prompt
import dvc.logger as logger

from dvc.command.base import CmdBase


class CmdGC(CmdBase):
    def run(self):
        msg = "this will remove all cache except the cache that is used in "
        if not self.args.all_branches and not self.args.all_tags:
            msg += "the current git branch"
        elif self.args.all_branches and not self.args.all_tags:
            msg += "all git branches"
        elif not self.args.all_branches and self.args.all_tags:
            msg += "all git tags"
        else:
            msg += "all git branches and all git tags"

        if self.args.repos is not None and len(self.args.repos) > 0:
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
            all_branches=self.args.all_branches,
            all_tags=self.args.all_tags,
            cloud=self.args.cloud,
            remote=self.args.remote,
            force=self.args.force,
            jobs=self.args.jobs,
            repos=self.args.repos,
        )
        return 0


def add_parser(subparsers, parent_parser):
    GC_HELP = "Collect garbage."
    gc_parser = subparsers.add_parser(
        "gc", parents=[parent_parser], description=GC_HELP, help=GC_HELP
    )
    gc_parser.add_argument(
        "-a",
        "--all-branches",
        action="store_true",
        default=False,
        help="Collect garbage for all branches.",
    )
    gc_parser.add_argument(
        "-T",
        "--all-tags",
        action="store_true",
        default=False,
        help="Collect garbage for all tags.",
    )
    gc_parser.add_argument(
        "-c",
        "--cloud",
        action="store_true",
        default=False,
        help="Collect garbage in remote repository.",
    )
    gc_parser.add_argument(
        "-r", "--remote", help="Remote repository to collect garbage in."
    )
    gc_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force garbage collection.",
    )
    gc_parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=None,
        help="Number of jobs to run simultaneously.",
    )
    gc_parser.add_argument(
        "-p",
        "--projects",
        dest="repos",
        type=str,
        nargs="*",
        default=None,
        help="Collect garbage for all given projects.",
    )
    gc_parser.set_defaults(func=CmdGC)
