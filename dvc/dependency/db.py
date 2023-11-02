import logging
import os
from contextlib import chdir, contextmanager, redirect_stdout
from typing import TYPE_CHECKING, Any, Dict, Union

from dvc.scm import SCM

from .base import Dependency

if TYPE_CHECKING:
    from dvc.stage import Stage

logger = logging.getLogger(__name__)


def log_streams():
    return redirect_stdout(StreamLogger(logging.DEBUG))


class StreamLogger:
    def __init__(self, level):
        self.level = level

    def write(self, message):
        logger.log(self.level, message)


class DbDependency(Dependency):
    PARAM_DB = "db"
    PARAM_PROFILE = "profile"
    PARAM_MODEL = "model"
    PARAM_QUERY = "query"
    PARAM_EXPORT_FORMAT = "export_format"

    DB_SCHEMA = {
        PARAM_DB: {
            PARAM_MODEL: str,
            PARAM_QUERY: str,
            PARAM_PROFILE: str,
            PARAM_EXPORT_FORMAT: str,
        }
    }

    def __init__(
        self, def_repo: Dict[str, Any], stage: "Stage", *args, **kwargs
    ):  # pylint: disable=super-init-not-called
        self.repo = stage.repo
        self.def_repo = def_repo
        self.db_info = kwargs.pop("db", {})
        self.fs = None
        self.fs_path = None
        self.def_path = None  # type: ignore[assignment]
        # super().__init__(stage, *args, **kwargs)

    def __repr__(self):
        return "{}:{}".format(
            self.__class__.__name__,
            "".join(f"{k}=={v}" for k, v in {**self.db_info, **self.def_repo}.items()),
        )

    def __str__(self):
        from .repo import RepoDependency

        repo = self.def_repo.get(RepoDependency.PARAM_URL)
        rev = self.def_repo.get(RepoDependency.PARAM_REV)

        db = self.db_info.get(self.PARAM_MODEL)
        if not db:
            from dvc.utils.humanize import truncate_text

            db = truncate_text(self.db_info.get(self.PARAM_QUERY, "[query]"), 50)

        repo_info = ""
        if repo:
            repo_info += repo
        if rev:
            repo_info += f"@{rev}"
        return db + (f"({repo_info})" if repo_info else "")

    @property
    def locked_rev(self):
        from .repo import RepoDependency

        return self.def_repo.get(RepoDependency.PARAM_REV_LOCK) or self.rev

    @property
    def rev(self):
        from .repo import RepoDependency

        return self.def_repo.get(RepoDependency.PARAM_REV)

    def workspace_status(self):
        current = self._get_clone(self.locked_rev or self.rev).get_rev()
        updated = self._get_clone(self.rev).get_rev()

        if current != updated:
            return {str(self): "update available"}
        return {}

    def status(self):
        return self.workspace_status()

    def save(self):
        from .repo import RepoDependency

        rev = self._get_clone(self.locked_rev or self.rev).get_rev()
        if self.def_repo.get(RepoDependency.PARAM_REV_LOCK) is None:
            self.def_repo[RepoDependency.PARAM_REV_LOCK] = rev

    def dumpd(self, **kwargs) -> Dict[str, Union[str, Dict[str, str]]]:
        from .repo import RepoDependency

        return {
            self.PARAM_DB: self.db_info,
            # pylint: disable-next=protected-access
            RepoDependency.PARAM_REPO: RepoDependency._dump_def_repo(self.def_repo),
        }

    def _get_clone(self, rev):
        from dvc.repo.open_repo import _cached_clone

        from .repo import RepoDependency

        url = self.def_repo.get(RepoDependency.PARAM_URL)
        repo_root = self.repo.root_dir if self.repo else os.getcwd()
        return SCM(_cached_clone(url, rev) if url else repo_root)

    def update(self, rev=None):
        from .repo import RepoDependency

        if rev:
            self.def_repo[RepoDependency.PARAM_REV] = rev
        else:
            rev = self.rev
        self.def_repo[RepoDependency.PARAM_REV_LOCK] = self._get_clone(rev).get_rev()

    def download(self, to, jobs=None, export_format=None):  # noqa: ARG002
        from dvc.ui import ui

        from .repo import RepoDependency

        repo = self._get_clone(self.locked_rev or self.rev)
        self.def_repo[RepoDependency.PARAM_REV_LOCK] = repo.get_rev()

        with chdir(repo.root_dir):
            self._download_dbt(to, export_format=export_format)
        ui.write(f"Saved file to {to}", styled=True)

    def _download_dbt(self, to, export_format=None):
        from funcy import log_durations

        from dvc.ui import ui

        @contextmanager
        def log_status(msg, log=logger.debug):
            with log_durations(log, msg), ui.status(msg):
                yield

        with log_status("Initializing dbt"), log_streams():
            from fal.dbt import FalDbt

            faldbt = FalDbt()

        if model := self.db_info.get(self.PARAM_MODEL):
            with log_status(f"Downloading {model}"), log_streams():
                model = faldbt.ref(model)
        elif query := self.db_info.get(self.PARAM_QUERY):
            with log_status("Executing sql query"), log_streams():
                model = faldbt.execute_sql(query)
        else:
            raise AssertionError("neither a query nor a model received")

        export_format = export_format or self.db_info.get(
            self.PARAM_EXPORT_FORMAT, "csv"
        )
        exporter = {
            "csv": model.to_csv,
            "json": model.to_json,
        }
        with log_status(f"Saving to {to}"), log_streams():
            exporter[export_format](to.fs_path)
