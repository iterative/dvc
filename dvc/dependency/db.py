import logging
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Union

from funcy import compact

from dvc.exceptions import DvcException
from dvc.scm import SCM

from .base import Dependency

if TYPE_CHECKING:
    from agate import Table

    from dvc.stage import Stage

logger = logging.getLogger(__name__)


PARAM_DB = "db"
PARAM_PROFILE = "profile"
PARAM_EXPORT_FORMAT = "export_format"  # TODO: kebab-case or snake_case?


@contextmanager
def log_status(msg, log=logger.debug):
    from funcy import log_durations

    from dvc.ui import ui

    with log_durations(log, msg), ui.status(msg):
        yield


@contextmanager
def chdir(path):
    wdir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(wdir)


def export_to(table: "Table", to: str, export_format: str = "csv") -> None:
    exporter = {"csv": table.to_csv, "json": table.to_json}
    return exporter[export_format](to)


class AbstractDependency(Dependency):
    """Dependency without workspace/fs/fs_path"""

    def __init__(
        self, stage: "Stage", info, *args, **kwargs
    ):  # pylint: disable=super-init-not-called
        self.repo = stage.repo
        self.stage = stage
        self.fs = None
        self.fs_path = None
        self.def_path = None  # type: ignore[assignment]
        self.info = info or {}


class DbDependency(AbstractDependency):
    PARAM_QUERY = "query"
    QUERY_SCHEMA = {PARAM_QUERY: str}

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

    def download(self, to, jobs=None, export_format=None):  # noqa: ARG002
        from dvc.utils.db import _profiles_dir, execute_sql

        db_info = self.info.get(PARAM_DB, {})
        query = db_info.get(self.PARAM_QUERY)
        if not query:
            raise DvcException("Cannot download: no query specified")

        db_config = self.repo.config.get("db", {})

        profiles_dir = _profiles_dir(self.repo.root_dir)
        profile = db_info.get(PARAM_PROFILE) or db_config.get(PARAM_PROFILE)

        target = self.target or db_config.get("target")

        with log_status("Executing query"):
            table = execute_sql(
                query, profiles_dir, self.repo.root_dir, profile, target=target
            )

        export_format = export_format or db_info.get(PARAM_EXPORT_FORMAT, "csv")
        with log_status(f"Saving to {to}"):
            return export_to(table, to.fs_path, export_format)


class DbtDependency(AbstractDependency):
    PARAM_MODEL = "model"
    PARAM_VERSION = "version"
    PARAM_PROJECT_DIR = "project_dir"  # TODO: kebab-case or snake_case?
    DBT_SCHEMA = {
        PARAM_MODEL: str,
        PARAM_VERSION: str,
        PARAM_PROJECT_DIR: str,
    }

    def __init__(
        self, def_repo: Dict[str, Any], stage: "Stage", info, *args, **kwargs
    ):  # pylint: disable=super-init-not-called
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
            # pylint: disable-next=protected-access
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

    def download(self, to, jobs=None, export_format=None):  # noqa: ARG002
        from dvc.ui import ui

        from .repo import RepoDependency

        project_dir = self.info.get(PARAM_DB, {}).get(self.PARAM_PROJECT_DIR, "")
        if self.def_repo:
            repo = self._get_clone(self.locked_rev or self.rev)
            self.def_repo[RepoDependency.PARAM_REV_LOCK] = repo.get_rev()
            root = wdir = repo.root_dir
        else:
            root = self.repo.root_dir
            wdir = self.stage.wdir

        project_path = os.path.join(wdir, project_dir) if project_dir else root
        with chdir(project_path):
            self._download_db(to, export_format=export_format)
        ui.write(f"Saved file to {to}", styled=True)

    def _download_db(self, to, version=None, export_format=None):
        from dvc.utils.db import get_model

        db_info = self.info.get(PARAM_DB, {})
        model = db_info.get(self.PARAM_MODEL)
        if not model:
            raise DvcException("Cannot download, no model specified")

        db_config = self.repo.config.get("db", {})
        version = version or db_info.get(self.PARAM_VERSION)
        profile = db_info.get(PARAM_PROFILE) or db_config.get(PARAM_PROFILE)
        target = self.target or db_info.get("target")

        with log_status("Downloading model"):
            table = get_model(model, version=version, profile=profile, target=target)

        export_format = export_format or db_info.get(PARAM_EXPORT_FORMAT, "csv")
        with log_status(f"Saving to {to}"):
            export_to(table, to.fs_path, export_format=export_format)


DB_SCHEMA = {
    PARAM_DB: {
        PARAM_PROFILE: str,
        PARAM_EXPORT_FORMAT: str,
        **DbDependency.QUERY_SCHEMA,
        **DbtDependency.DBT_SCHEMA,
    },
}
