import argparse

from funcy import compact, merge

from dvc.cli import completion
from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdTestDb(CmdBase):
    def run(self):
        from dvc.database import get_client
        from dvc.database.dbt_utils import DBT_PROJECT_FILE, is_dbt_project
        from dvc.dependency.db import _get_dbt_config
        from dvc.exceptions import DvcException

        connection = self.args.conn

        db_config = self.repo.config.get("db", {})
        config = db_config.get(connection, {})
        if connection and not config:
            raise DvcException(f"connection {connection} not found in config")

        cli_config = compact(
            {
                "url": self.args.url,
                "username": self.args.username,
                "password": self.args.password,
            }
        )
        conn_config = merge(config, cli_config)

        cli_dbt_config = compact(
            {"profile": self.args.profile, "target": self.args.target}
        )
        dbt_config = merge(_get_dbt_config(self.repo.config), cli_dbt_config)

        project_dir = self.repo.root_dir
        if not (conn_config or dbt_config):
            if not self.args.dbt_conn:
                raise DvcException(
                    "no config set; provide arguments or set a configuration"
                )

            if is_dbt_project(project_dir):
                ui.write("Using", DBT_PROJECT_FILE, "for testing", styled=True)
            else:
                raise DvcException(
                    f"no config set and {DBT_PROJECT_FILE} is missing; "
                    "provide arguments or set a configuration"
                )

        adapter = get_client(conn_config, project_dir=project_dir, **dbt_config)
        with adapter as db:
            ui.write(f"Testing with {db}", styled=True)

            creds = getattr(db, "creds", {})
            for k, v in creds.items():
                ui.write("\t", f"{k}:", v, styled=True)

            if creds:
                ui.write()

            db.test_connection()

        ui.write("Connection successful", styled=True)


class CmdImportDb(CmdBase):
    def run(self):
        from dvc.exceptions import InvalidArgumentError

        if self.args.table or self.args.sql:
            arg = "--table" if self.args.table else "--sql"
            options = {
                "url": self.args.url,
                "rev": self.args.rev,
                "project_dir": self.args.project_dir,
            }
            opt = next((o for o, v in options.items() if v), None)
            if opt:
                raise InvalidArgumentError(f"argument {opt}: not allowed with {arg}")

            if not self.args.conn and not self.args.dbt_conn:
                raise InvalidArgumentError(f"{arg} requires --conn")
        if self.args.model and self.args.conn:
            raise InvalidArgumentError("argument --model: not allowed with --conn")

        self.repo.imp_db(
            url=self.args.url,
            rev=self.args.rev,
            project_dir=self.args.project_dir,
            sql=self.args.sql,
            table=self.args.table,
            model=self.args.model,
            profile=self.args.profile,
            target=self.args.target,
            output_format=self.args.output_format,
            out=self.args.out,
            force=self.args.force,
            connection=self.args.conn,
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
        "--url",
        help=argparse.SUPPRESS,
        # help="Location of dbt repository",
    )
    import_parser.add_argument(
        "--rev",
        nargs="?",
        help=argparse.SUPPRESS,
        # help="Git revision (e.g. SHA, branch, tag)",
        metavar="<commit>",
    )
    import_parser.add_argument(
        "--project-dir",
        nargs="?",
        help=argparse.SUPPRESS,
        # help="Subdirectory to the dbt project location",
    )

    group = import_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="SQL query to snapshot")
    group.add_argument("--table", help="Table to snapshot")
    group.add_argument(
        "--model",
        help=argparse.SUPPRESS,
        # help="Model name to download",
    )
    import_parser.add_argument(
        "--profile",
        help=argparse.SUPPRESS,
        # help="Profile to use",
    )
    import_parser.add_argument(
        "--target",
        help=argparse.SUPPRESS,
        # help="Target to use",
    )
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
    import_parser.add_argument(
        "--conn",
        nargs="?",
        help="Database connection to use, needs to be set in config",
    )
    import_parser.add_argument(
        "--dbt-conn",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
        # help="Use dbt connection",
    )

    import_parser.set_defaults(func=CmdImportDb)

    TEST_DB_HELP = "Test the database connection"
    test_db_parser = subparsers.add_parser(
        "test-db",
        parents=[parent_parser],
        description=append_doc_link(TEST_DB_HELP, "test-db"),
        add_help=False,
    )
    test_db_parser.add_argument("--conn")
    test_db_parser.add_argument("--dbt-conn", action="store_true", default=False)
    test_db_parser.add_argument("--url")
    test_db_parser.add_argument("--password")
    test_db_parser.add_argument("--username")
    test_db_parser.add_argument("--profile")
    test_db_parser.add_argument("--target")
    test_db_parser.set_defaults(func=CmdTestDb)
