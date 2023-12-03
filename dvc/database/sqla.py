from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.engine import make_url as _make_url
from sqlalchemy.exc import NoSuchModuleError

from dvc.exceptions import DvcException
from dvc.log import logger

from .serializer import PandasSQLSerializer

logger = logger.getChild(__name__)


def make_url(url: Union[URL, str], **kwargs: Any) -> URL:
    return _make_url(url).set(**kwargs)


def url_from_config(config: Union[str, URL, Dict[str, str]]) -> URL:
    if isinstance(config, (str, URL)):
        return make_url(config)
    return make_url(**config)


@dataclass
class SQLAlchemyAdapter:
    engine: Engine

    @contextmanager
    def query(self, sql: str) -> Iterator[PandasSQLSerializer]:
        with self.engine.connect().execution_options(stream_results=True) as conn:
            yield PandasSQLSerializer(text(sql), conn)

    def test_connection(self, onerror: Optional[Callable[[], Any]] = None) -> None:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("select 1"))
        except Exception as exc:
            if callable(onerror):
                onerror()
            logger.exception(
                "dvc was unable to connect to the specified database. "
                "Please check your database credentials and try again.",
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
def adapter(
    url_or_config: Union[str, URL, Dict[str, str]], **engine_kwargs: Any
) -> Iterator[SQLAlchemyAdapter]:
    url = url_from_config(url_or_config)
    with handle_error(url):
        engine = create_engine(url, **engine_kwargs)
    try:
        yield SQLAlchemyAdapter(engine)
    finally:
        engine.dispose()
