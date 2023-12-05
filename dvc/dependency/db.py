import os
from contextlib import contextmanager, nullcontext
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, Optional, Union

from funcy import compact, log_durations

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.scm import SCM

from .base import Dependency

if TYPE_CHECKING:
    from rich.status import Status

    from dvc.output import Output
    from dvc.stage import Stage

logger = logger.getChild(__name__)


PARAM_DB = "db"
PARAM_PROFILE = "profile"
PARAM_FILE_FORMAT = "file_format"


def _get_dbt_config(config: Dict) -> Dict:
    conf = config.get("feature", {})
    pref = "dbt_"
    return {k.lstrip(pref): v for k, v in conf.items() if k.startswith(pref)}


@contextmanager
def log_status(
    msg, status: Optional["Status"] = None, log=logger.debug
) -> Iterator["Status"]:
    from funcy import log_durations

    from dvc.ui import ui

    with log_durations(log, msg), status or ui.status(msg) as st:
        st.update(msg)
        yield st


@contextmanager
def chdir(path):
    wdir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(wdir)


@contextmanager
def download_progress(to: "Output") -> Iterator[Callable[[int], Any]]:
    from dvc.ui import ui
    from dvc.ui._rich_progress import DbDownloadProgress

    with log_durations(logger.debug, f"Saving to {to}"), DbDownloadProgress(
        console=ui.error_console,
    ) as progress:
        task = progress.add_task("Saving", total=None, output=to)
        yield lambda n: progress.advance(task, advance=n)
        progress.update(task, description="Saved", total=0)


class AbstractDependency(Dependency):
    """Dependency without workspace/fs/fs_path"""

    def __init__(self, stage: "Stage", info: Dict[str, Any], *args, **kwargs):
        self.repo = stage.repo
        self.stage = stage
        self.fs = None
        self.fs_path = None
        self.def_path = None  # type: ignore[assignment]
        self.info = info or {}


class DbDependency(AbstractDependency):
    PARAM_QUERY = "query"
    PARAM_CONNECTION = "connection"
    QUERY_SCHEMA = {PARAM_QUERY: str, PARAM_CONNECTION: str}

    def __init__(self, stage: "Stage", info, *args, **kwargs):
        super().__init__(stage, info, *args, **kwargs)
        self.target = None

    def __repr__(self):
        return "{}:{}".format(
            self.__class__.__name__, "".join(f"{k}=={v}" for k, v in self.info.items())
        )

    def __str__(self):
        from dvc.utils.humanize import truncate_text

        db_info = self.info.get(PARAM_DB, {})
        query = db_info.get(self.PARAM_QUERY, "[query]")
        return truncate_text(query, 50)

    def workspace_status(self):
        return False  # no workspace to check

    def status(self):
        return self.workspace_status()

    def save(self):
        """nothing to save."""

    def dumpd(self, **kwargs):
        db_info = compact(self.info.get(PARAM_DB, {}))
        return {PARAM_DB: db_info} if db_info else {}

    def update(self, rev=None):
        """nothing to update."""

    def download(
        self,
        to: "Output",
        jobs: Optional[int] = None,  # noqa: ARG002
        file_format: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        from dvc.database import export, get_adapter

        db_info = self.info.get(PARAM_DB, {})
        query = db_info.get(self.PARAM_QUERY)
        if not query:
            raise DvcException("Cannot download: no query specified")

        dbt_config = _get_dbt_config(self.repo.config)
        profile = db_info.get(PARAM_PROFILE) or dbt_config.get(PARAM_PROFILE)
        target = self.target or dbt_config.get("target")

        connection = db_info.get(self.PARAM_CONNECTION)
        db_config = self.repo.config.get("db", {})
        config = db_config.get(connection)
        if connection and not config:
            raise DvcException(f"connection {connection} not found in config")

        project_dir = self.repo.root_dir
        with get_adapter(
            config, project_dir=project_dir, profile=profile, target=target
        ) as db:
            logger.debug("using adapter: %s", db)
            with log_status("Testing connection") as status:
                db.test_connection(onerror=status.stop)

            file_format = file_format or db_info.get(PARAM_FILE_FORMAT, "csv")
            assert file_format
            with log_status("Executing query") as status, db.query(query) as serializer:
                status.stop()
                logger.debug("using serializer: %s", serializer)
                with download_progress(to) as progress:
                    return export(
                        serializer, to.fs_path, format=file_format, progress=progress
                    )


class DbtDependency(AbstractDependency):
    PARAM_MODEL = "model"
    PARAM_VERSION = "version"
    PARAM_PROJECT_DIR = "project_dir"
    DBT_SCHEMA = {
        PARAM_MODEL: str,
        PARAM_VERSION: str,
        PARAM_PROJECT_DIR: str,
    }

    def __init__(self, def_repo: Dict[str, Any], stage: "Stage", info, *args, **kwargs):
        self.def_repo = def_repo or {}
        self.target = None
        super().__init__(stage, info, *args, **kwargs)

    def __repr__(self):
        return "{}:{}".format(
            self.__class__.__name__,
            "".join(f"{k}=={v}" for k, v in {**self.def_repo, **self.info}.items()),
        )

    def __str__(self):
        from .repo import RepoDependency

        repo = self.def_repo.get(RepoDependency.PARAM_URL)
        rev = self.def_repo.get(RepoDependency.PARAM_REV)

        db_info = self.info.get(PARAM_DB, {})
        db = db_info.get(self.PARAM_MODEL, "")
        project_dir = db_info.get(self.PARAM_PROJECT_DIR, "")
        repo_info = ""
        if repo:
            repo_info += repo
        if rev:
            repo_info += f"@{rev}"
        if project_dir:
            repo_info += f":/{project_dir}"
        return db + (f"({repo_info})" if repo_info else "")

    @property
    def locked_rev(self):
        from .repo import RepoDependency

        return self.def_repo.get(RepoDependency.PARAM_REV_LOCK)

    @property
    def rev(self):
        from .repo import RepoDependency

        return self.def_repo.get(RepoDependency.PARAM_REV)

    def workspace_status(self):
        if not self.def_repo:
            return

        current = self._get_clone(self.locked_rev or self.rev).get_rev()
        updated = self._get_clone(self.rev).get_rev()
        if current != updated:
            return {str(self): "update available"}
        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        from .repo import RepoDependency

        if not self.def_repo:
            return

        rev = self._get_clone(self.locked_rev or self.rev).get_rev()
        if self.def_repo.get(RepoDependency.PARAM_REV_LOCK) is None:
            self.def_repo[RepoDependency.PARAM_REV_LOCK] = rev

    def dumpd(self, **kwargs) -> Dict[str, Union[str, Dict[str, str]]]:
        from .repo import RepoDependency

        def_repo = {}
        if self.def_repo:
            def_repo = RepoDependency._dump_def_repo(self.def_repo)

        db_info = compact(self.info.get(PARAM_DB, {}))
        return compact({RepoDependency.PARAM_REPO: def_repo, PARAM_DB: db_info})

    def _get_clone(self, rev):
        from dvc.repo.open_repo import _cached_clone

        from .repo import RepoDependency

        url = self.def_repo.get(RepoDependency.PARAM_URL)
        repo_root = self.repo.root_dir if self.repo else os.getcwd()
        return SCM(_cached_clone(url, rev) if url else repo_root)

    def update(self, rev=None):
        from .repo import RepoDependency

        if not self.def_repo:
            return

        if rev:
            self.def_repo[RepoDependency.PARAM_REV] = rev
        else:
            rev = self.rev
        self.def_repo[RepoDependency.PARAM_REV_LOCK] = self._get_clone(rev).get_rev()

    def download(
        self,
        to: "Output",
        jobs: Optional[int] = None,  # noqa: ARG002
        file_format: Optional[str] = None,
    ) -> None:
        from .repo import RepoDependency

        project_dir = self.info.get(PARAM_DB, {}).get(self.PARAM_PROJECT_DIR, "")
        if self.def_repo:
            ctx = repo = self._get_clone(self.locked_rev or self.rev)
            self.def_repo[RepoDependency.PARAM_REV_LOCK] = repo.get_rev()
            root = wdir = repo.root_dir
        else:
            ctx = nullcontext()
            root = self.repo.root_dir
            wdir = self.stage.wdir

        project_path = os.path.join(wdir, project_dir) if project_dir else root
        with ctx, chdir(project_path):
            self._download_db(to, file_format=file_format)

    def _download_db(
        self,
        to: "Output",
        version: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> None:
        from dvc.database import export, get_model

        db_info = self.info.get(PARAM_DB, {})
        model = db_info.get(self.PARAM_MODEL)
        version = version or db_info.get(self.PARAM_VERSION)
        file_format = file_format or db_info.get(PARAM_FILE_FORMAT) or "csv"
        assert file_format
        if not model:
            raise DvcException("Cannot download, no model specified")

        dbt_config = _get_dbt_config(self.repo.config)
        profile = db_info.get(PARAM_PROFILE) or dbt_config.get(PARAM_PROFILE)
        target = self.target or db_info.get("target") or dbt_config.get("target")

        with log_status("Downloading model"):
            serializer = get_model(
                model, version=version, profile=profile, target=target
            )
        # NOTE: we keep everything in memory, and then export it out later.
        with download_progress(to) as progress:
            export(serializer, to.fs_path, format=file_format, progress=progress)


DB_SCHEMA = {
    PARAM_DB: {
        PARAM_PROFILE: str,
        PARAM_FILE_FORMAT: str,
        **DbDependency.QUERY_SCHEMA,
        **DbtDependency.DBT_SCHEMA,
    },
}
