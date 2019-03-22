from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdCommit(CmdBase):
    def run(self):
        if not self.args.targets:
            self.args.targets = [None]

        for target in self.args.targets:
            try:
                self.repo.commit(
                    target,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                    force=self.args.force,
                )
            except DvcException:
                logger.error(
                    "failed to commit{}".format(
                        (" " + target) if target else ""
                    )
                )
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    COMMIT_HELP = (
        "Record changes to the repository.\n"
        "documentation: https://man.dvc.org/commit"
    )
    commit_parser = subparsers.add_parser(
        "commit",
        parents=[parent_parser],
        description=COMMIT_HELP,
        help=COMMIT_HELP,
    )
    commit_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Commit even if checksums for dependencies/outputs changed.",
    )
    commit_parser.add_argument(
        "-d",
        "--with-deps",
        action="store_true",
        default=False,
        help="Commit all dependencies of the specified target.",
    )
    commit_parser.add_argument(
        "-R",
        "--recursive",
        action="store_true",
        default=False,
        help="Commit cache for subdirectories of the specified directory.",
    )
    commit_parser.add_argument(
        "targets", nargs="*", default=None, help="DVC files."
    )
    commit_parser.set_defaults(func=CmdCommit)
