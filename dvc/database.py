import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from sqlalchemy import create_engine  # type: ignore[import]
from sqlalchemy.engine import make_url as _make_url  # type: ignore[import]
from sqlalchemy.exc import NoSuchModuleError  # type: ignore[import]

from dvc import env
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.types import StrOrBytesPath
from dvc.utils import env2bool

if TYPE_CHECKING:
    from sqlalchemy.engine import URL, Connectable, Engine
    from sqlalchemy.sql.expression import Selectable  # type: ignore[import]


logger = logger.getChild(__name__)


def noop(_):
    pass


def make_url(url: Union["URL", str], **kwargs: Any) -> "URL":
    return _make_url(url).set(**kwargs)


def url_from_config(config: Union[str, "URL", dict[str, str]]) -> "URL":
    if isinstance(config, dict):
        return make_url(**config)
    return make_url(config)


@contextmanager
def atomic_file(file: StrOrBytesPath, mode: str = "w+b"):
    head, tail = os.path.split(os.fsdecode(file))
    with NamedTemporaryFile(mode, prefix=tail + "-", dir=head, delete=False) as f:
        yield f
    os.replace(f.name, file)


@dataclass
class Serializer:
    sql: "Union[str, Selectable]"
    con: "Union[str, Connectable]"
    chunksize: int = 10_000

    def to_csv(self, file: StrOrBytesPath, progress=noop):
        import pandas as pd

        idfs = pd.read_sql(self.sql, self.con, chunksize=self.chunksize)
        with atomic_file(file) as f:
            for i, df in enumerate(idfs):
                df.to_csv(f, header=i == 0, index=False)
                progress(len(df))

    def to_json(self, file: StrOrBytesPath, progress=noop):  # noqa: ARG002
        import pandas as pd

        df = pd.read_sql(self.sql, self.con)
        with atomic_file(file) as f:
            df.to_json(f, orient="records")

    def export(self, file: StrOrBytesPath, format: str = "csv", progress=noop):  # noqa: A002
        if format == "json":
            return self.to_json(file, progress=progress)
        return self.to_csv(file, progress=progress)


@dataclass
class Client:
    engine: "Engine"

    def test_connection(self, onerror: Optional[Callable[[], Any]] = None) -> None:
        try:
            with self.engine.connect() as conn:
                conn.exec_driver_sql("select 1")
        except Exception as exc:
            if callable(onerror):
                onerror()
            logger.exception(
                "Could not connect to the database. "
                "Check your database credentials and try again.",
                exc_info=False,
            )
            raise DvcException("The database returned the following error") from exc

    def export(
        self,
        sql: "Union[str, Selectable]",
        file: StrOrBytesPath,
        format: str = "csv",  # noqa: A002
        progress=noop,
    ) -> None:
        con = self.engine.connect()
        if format == "csv":
            con = con.execution_options(stream_results=True)  # use server-side cursors

        with con:
            serializer = Serializer(sql, con)
            return serializer.export(file, format=format, progress=progress)


@contextmanager
def handle_error(url: "URL"):
    try:
        yield
    except (ModuleNotFoundError, NoSuchModuleError) as e:
        # TODO: write installation instructions
        driver = url.drivername
        raise DvcException(f"Could not load database driver for {driver!r}") from e


@contextmanager
def client(
    url_or_config: Union[str, "URL", dict[str, str]], **engine_kwargs: Any
) -> Iterator[Client]:
    url = url_from_config(url_or_config)
    echo = env2bool(env.DVC_SQLALCHEMY_ECHO, False)
    engine_kwargs.setdefault("echo", echo)

    with handle_error(url):
        engine = create_engine(url, **engine_kwargs)

    try:
        yield Client(engine)
    finally:
        engine.dispose()
