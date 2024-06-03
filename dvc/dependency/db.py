from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Optional

from funcy import compact, log_durations

from dvc.exceptions import DvcException
from dvc.log import logger

from .base import Dependency

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.stage import Stage

logger = logger.getChild(__name__)


@contextmanager
def download_progress(to: "Output") -> Iterator[Callable[[int], Any]]:
    from dvc.ui import ui
    from dvc.ui._rich_progress import DbDownloadProgress

    with (
        log_durations(logger.debug, f"Saving to {to}"),
        DbDownloadProgress(
            console=ui.error_console,
        ) as progress,
    ):
        task = progress.add_task("Saving", total=None, output=to)
        yield lambda n: progress.advance(task, advance=n)
        progress.update(task, description="Saved", total=0)


class AbstractDependency(Dependency):
    """Dependency without workspace/fs/fs_path"""

    def __init__(self, stage: "Stage", info: dict[str, Any], *args, **kwargs):
        self.repo: Repo = stage.repo
        self.stage = stage
        self.fs = None
        self.fs_path = None
        self.def_path = None  # type: ignore[assignment]
        self.info = info or {}


class DbDependency(AbstractDependency):
    PARAM_CONNECTION = "connection"
    PARAM_DB = "db"
    PARAM_QUERY = "query"
    PARAM_TABLE = "table"
    PARAM_FILE_FORMAT = "file_format"
    DB_SCHEMA: ClassVar[dict] = {
        PARAM_DB: {
            PARAM_QUERY: str,
            PARAM_CONNECTION: str,
            PARAM_FILE_FORMAT: str,
            PARAM_TABLE: str,
        }
    }

    def __init__(self, stage: "Stage", info, *args, **kwargs):
        super().__init__(stage, info, *args, **kwargs)
        self.db_info: dict[str, str] = self.info.get(self.PARAM_DB, {})
        self.connection = self.db_info.get(self.PARAM_CONNECTION)

    @property
    def sql(self) -> Optional[str]:
        return self.db_info.get(self.PARAM_QUERY) or self.db_info.get(self.PARAM_TABLE)

    def __repr__(self):
        return "{}:{}".format(
            self.__class__.__name__, "".join(f"{k}=={v}" for k, v in self.info.items())
        )

    def __str__(self):
        from dvc.utils.humanize import truncate_text

        return truncate_text(self.sql or "", 50)

    def workspace_status(self):
        return False  # no workspace to check

    def status(self):
        return self.workspace_status()

    def save(self):
        """nothing to save."""

    def dumpd(self, **kwargs):
        db_info = compact(self.db_info)
        return {self.PARAM_DB: db_info} if db_info else {}

    def update(self, rev=None):
        """nothing to update."""

    def download(
        self,
        to: "Output",
        jobs: Optional[int] = None,  # noqa: ARG002
        file_format: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        from dvc.database import client
        from dvc.ui import ui

        sql = self.sql
        if not sql:
            raise DvcException("Cannot download: no query or table specified")

        db_config = self.repo.config.get(self.PARAM_DB, {})
        config = db_config.get(self.connection)
        if not config:
            raise DvcException(f"connection {self.connection} not found in config")

        file_format = file_format or self.db_info.get(self.PARAM_FILE_FORMAT, "csv")
        assert file_format
        with client(config) as db:
            msg = "Testing connection"
            with log_durations(logger.debug, msg), ui.status(msg) as status:
                db.test_connection(onerror=status.stop)
            with download_progress(to) as progress:
                db.export(sql, to.fs_path, format=file_format, progress=progress)
