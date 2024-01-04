from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, ContextManager, Dict, Iterator, Optional, Union

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.engine import make_url as _make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.sql import table
from sqlalchemy.sql.expression import Selectable

from dvc import env
from dvc.exceptions import DvcException
from dvc.log import logger
from dvc.utils import env2bool

from .serializer import PandasSQLSerializer

logger = logger.getChild(__name__)


def make_url(url: Union[URL, str], **kwargs: Any) -> URL:
    return _make_url(url).set(**kwargs)


def url_from_config(config: Union[str, URL, Dict[str, str]]) -> URL:
    if isinstance(config, (str, URL)):
        return make_url(config)
    return make_url(**config)


@dataclass
class SQLAlchemyClient:
    engine: Engine

    @contextmanager
    def query(self, sql: Union[str, Selectable]) -> Iterator[PandasSQLSerializer]:
        with self.engine.connect().execution_options(stream_results=True) as conn:
            yield PandasSQLSerializer(sql, conn)

    def table(
        self, name: str, limit: Optional[int] = None
    ) -> ContextManager[PandasSQLSerializer]:
        stmt = select("*").select_from(table(name)).limit(limit)
        return self.query(stmt)

    def test_connection(self, onerror: Optional[Callable[[], Any]] = None) -> None:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("select 1"))
        except Exception as exc:
            if callable(onerror):
                onerror()
            logger.exception(
                "Could not connect to the database. "
                "Check your database credentials and try again.",
                exc_info=False,
            )
            raise DvcException("The database returned the following error") from exc


@contextmanager
def handle_error(url: URL):
    try:
        yield
    except (ModuleNotFoundError, NoSuchModuleError) as e:
        # TODO: write installation instructions
        raise DvcException(
            f"Could not load database driver for {url.drivername!r}"
        ) from e


@contextmanager
def client(
    url_or_config: Union[str, URL, Dict[str, str]], **engine_kwargs: Any
) -> Iterator[SQLAlchemyClient]:
    url = url_from_config(url_or_config)
    with handle_error(url):
        echo = env2bool(env.DVC_SQLALCHEMY_ECHO, False)
        engine_kwargs.setdefault("echo", echo)
        engine = create_engine(url, **engine_kwargs)
    try:
        yield SQLAlchemyClient(engine)
    finally:
        engine.dispose()
