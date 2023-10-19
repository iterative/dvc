import argparse
import logging

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

logger = logging.getLogger(__name__)


class CmdImportDb(CmdBase):
    def run(self):
        from fal.dbt import FalDbt
        from funcy import print_durations

        from dvc.repo.open_repo import _cached_clone

        clone = _cached_clone(self.args.url, self.args.rev)
        faldbt = FalDbt(profiles_dir="~/.dbt", project_dir=clone)

        if not self.args.sql:
            name = self.args.to_materialize
            out = self.args.out or f"{name}.csv"
            with print_durations(f"ref {name}"), ui.status(f"Downloading {name}"):
                model = faldbt.ref(name)
        else:
            query = self.args.to_materialize
            out = self.args.out or "result.csv"
            with print_durations(f"execute_sql {query}"), ui.status(
                "Executing sql query"
            ):
                model = faldbt.execute_sql(query)

        with print_durations(f"to_csv {out}"), ui.status(f"Saving to {out}"):
            model.to_csv(out)

        ui.write(f"Saved file to {out}", styled=True)


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = (
        "Download file or directory tracked by DVC or by Git "
        "into the workspace, and track it."
    )

    import_parser = subparsers.add_parser(
        "import-db",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import"),
        help=IMPORT_HELP,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    import_parser.add_argument(
        "url", help="Location of DVC or Git repository to download from"
    )
    import_parser.add_argument(
        "to_materialize", help="Name of the dbt model or SQL query (if --sql)"
    )
    import_parser.add_argument(
        "--sql",
        help="is a sql query",
        action="store_true",
        default=False,
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
    import_parser.add_argument(
        "--rev",
        nargs="?",
        help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )

    import_parser.set_defaults(func=CmdImportDb)
