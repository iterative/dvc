from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdCommit(CmdBase):
    def run(self):
        from dvc.exceptions import DvcException

        if not self.args.targets:
            self.args.targets = [None]

        for target in self.args.targets:
            try:
                self.repo.commit(
                    target,
                    with_deps=self.args.with_deps,
                    recursive=self.args.recursive,
                    force=self.args.force,
                    relink=self.args.relink,
                )
            except DvcException:
                logger.exception("failed to commit%s", (" " + target) if target else "")
                return 1
        return 0


def add_parser(subparsers, parent_parser):
    COMMIT_HELP = (
        "Record changes to files or directories tracked by DVC"
        " by storing the current versions in the cache."
    )

    commit_parser = subparsers.add_parser(
        "commit",
        parents=[parent_parser],
        description=append_doc_link(COMMIT_HELP, "commit"),
        help=COMMIT_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )
    commit_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help=(
            "Commit data even if hash values for dependencies or "
            "outputs did not change."
        ),
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
        "--no-relink",
        dest="relink",
        action="store_false",
        help="Don't recreate links from cache to workspace.",
    )
    commit_parser.set_defaults(relink=True)
    commit_parser.add_argument(
        "targets",
        nargs="*",
        help=(
            "Limit command scope to these tracked files/directories, "
            ".dvc files and stage names."
        ),
    ).complete = completion.DVCFILES_AND_STAGE
    commit_parser.set_defaults(func=CmdCommit)
