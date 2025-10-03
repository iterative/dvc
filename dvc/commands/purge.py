import os

from dvc.cli import formatter
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdPurge(CmdBase):
    def run(self):
        if not self.args.dry_run:
            msg = "This will permanently remove local DVC-tracked outputs "
        else:
            msg = "This will show what local DVC-tracked outputs would be removed "
        if self.args.targets:
            msg += "for the following targets:\n  - " + "\n  - ".join(
                [os.path.abspath(t) for t in self.args.targets]
            )
        else:
            msg += "for the entire workspace."

        if self.args.recursive:
            msg += "\nRecursive purge is enabled."

        if self.args.dry_run:
            msg += "\n(dry-run: showing what would be removed, no changes)."

        logger.warning(msg)

        if (
            not self.args.force
            and not self.args.dry_run
            and not self.args.yes
            and not ui.confirm("Are you sure you want to proceed?")
        ):
            return 1

        # Call repo API
        self.repo.purge(
            targets=self.args.targets,
            recursive=self.args.recursive,
            force=self.args.force,
            dry_run=self.args.dry_run,
        )
        return 0


def add_parser(subparsers, parent_parser):
    PURGE_HELP = "Remove tracked outputs and their cache."
    PURGE_DESCRIPTION = (
        "Removes cache objects and workspace copies of DVC-tracked outputs.\n"
        "Metadata remains intact, and non-DVC files are untouched."
    )
    purge_parser = subparsers.add_parser(
        "purge",
        parents=[parent_parser],
        description=append_doc_link(PURGE_DESCRIPTION, "purge"),
        help=PURGE_HELP,
        formatter_class=formatter.RawDescriptionHelpFormatter,
    )

    purge_parser.add_argument(
        "targets",
        nargs="*",
        help="Optional list of files/directories to purge (default: entire repo).",
    )
    purge_parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        default=False,
        help="Recursively purge directories.",
    )
    purge_parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Only print what would be removed without actually removing.",
    )
    purge_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force purge, bypassing safety checks and prompts.",
    )
    purge_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Do not prompt for confirmation (respects saftey checks).",
    )

    purge_parser.set_defaults(func=CmdPurge)
