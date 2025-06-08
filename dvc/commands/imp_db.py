from dvc.cli import completion, formatter
from dvc.cli.command import CmdBase, CmdBaseNoRepo
from dvc.cli.utils import append_doc_link
from dvc.log import logger
from dvc.ui import ui

logger = logger.getChild(__name__)


class CmdTestDb(CmdBaseNoRepo):
    def run(self):
        from dvc.config import Config
        from dvc.database import client
        from dvc.exceptions import DvcException

        connection = self.args.conn
        db_config = Config.from_cwd().get("db", {})
        if connection not in db_config:
            raise DvcException(f"connection {connection} not found in config")

        config = db_config.get(connection, {})
        if self.args.url:
            config["url"] = self.args.url
        if self.args.username:
            config["username"] = self.args.username
        if self.args.password:
            config["password"] = self.args.password
        with client(config) as db:
            ui.write(f"Testing with {db}", styled=True)
            db.test_connection()
        ui.write("Connection successful", styled=True)


class CmdImportDb(CmdBase):
    def run(self):
        self.repo.imp_db(
            sql=self.args.sql,
            table=self.args.table,
            output_format=self.args.output_format,
            out=self.args.out,
            force=self.args.force,
            connection=self.args.conn,
        )
        return 0


def add_parser(subparsers, parent_parser):
    IMPORT_HELP = "Snapshot a table or a SQL query result to a CSV/JSON format"
    import_parser = subparsers.add_parser(
        "import-db",
        parents=[parent_parser],
        description=append_doc_link(IMPORT_HELP, "import-db"),
        help=IMPORT_HELP,
        formatter_class=formatter.RawTextHelpFormatter,
    )
    group = import_parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sql", help="SQL query to snapshot")
    group.add_argument("--table", help="Table to snapshot")
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
        required=True,
        help="Database connection to use, needs to be set in config",
    )

    import_parser.set_defaults(func=CmdImportDb)

    TEST_DB_HELP = "Test the database connection"
    test_db_parser = subparsers.add_parser(
        "test-db",
        parents=[parent_parser],
        description=append_doc_link(TEST_DB_HELP, "test-db"),
        add_help=False,
    )
    test_db_parser.add_argument("--conn", required=True)
    test_db_parser.add_argument("--url")
    test_db_parser.add_argument("--password")
    test_db_parser.add_argument("--username")
    test_db_parser.set_defaults(func=CmdTestDb)
