import argparse

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger

logger = logger.getChild(__name__)


class CmdImportDb(CmdBase):
    def run(self):
        if not (self.args.sql or self.args.model):
            raise argparse.ArgumentTypeError("Either of --sql or --model is required.")

        self.repo.imp_db(
            url=self.args.url,
            rev=self.args.rev,
            project_dir=self.args.project_dir,
            sql=self.args.sql,
            model=self.args.model,
            profile=self.args.profile,
            target=self.args.target,
            output_format=self.args.output_format,
            out=self.args.out,
            force=self.args.force,
        )
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Download file or directory tracked by DVC or by Git "
        "into the workspace, and track it."
    )

    import_parser = subparsers.add_parser(
        "import-db",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import"),
        add_help=False,
    )
    import_parser.add_argument(
        "--url", help="Location of DVC or Git repository to download from"
    )
    import_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    import_parser.add_argument(
        "--project-dir", nargs="?", help="Subdirectory to the dbt project location"
    )

    group = import_parser.add_mutually_exclusive_group()
    group.add_argument(
        "--sql",
        help="SQL query",
    )
    group.add_argument(
        "--model",
        help="Model name to download",
    )
    import_parser.add_argument("--profile", help="Profile to use")
    import_parser.add_argument("--target", help="Target to use")
    import_parser.add_argument(
        "--output-format",
        default="csv",
        const="csv",
        nargs="?",
        choices=["csv", "json"],
        help="Export format",
    )
    import_parser.add_argument(
        "-o",
        "--out",
        nargs="?",
        help="Destination path to download files to",
        metavar="<path>",
    ).complete = completion.FILE
    import_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Override destination file or folder if exists.",
    )

    import_parser.set_defaults(func=CmdImportDb)
