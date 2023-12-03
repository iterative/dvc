import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, Optional

from attrs import define, field

from dvc.exceptions import DvcException
from dvc.log import logger

from .dbt_utils import check_dbt
from .serializer import AgateSerializer

if TYPE_CHECKING:
    from dbt.adapters.sql.impl import SQLAdapter


logger = logger.getChild(__name__)


@define
class DbtAdapter:
    adapter: "SQLAdapter" = field(repr=lambda o: type(o).__qualname__)
    creds: Dict[str, Any] = field(repr=False)

    def query(self, sql: str) -> AgateSerializer:
        with self.adapter.connection_named("execute"):
            _, table = self.adapter.execute(sql, fetch=True)
            return AgateSerializer(table)

    def test_connection(self, onerror: Optional[Callable[[], Any]] = None) -> None:
        with self.adapter.connection_named("debug"):
            try:
                self.adapter.debug_query()
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
@check_dbt("query")
def adapter(
    project_dir: Optional[str] = None,
    profiles_dir: Optional[str] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> Iterator["DbtAdapter"]:
    from dbt.adapters import factory as adapters_factory
    from dbt.adapters.sql import SQLAdapter

    from .dbt_utils import get_or_build_profile, get_profiles_dir, init_dbt

    profiles_dir = profiles_dir or get_profiles_dir(project_dir)
    flags = init_dbt(profiles_dir, os.getcwd(), target=target)

    with flags, adapters_factory.adapter_management():
        # likely invalid connection profile or no adapter
        profile_obj = get_or_build_profile(project_dir, profile, target)

        adapters_factory.register_adapter(profile_obj)  # type: ignore[arg-type]
        adapter = adapters_factory.get_adapter(profile_obj)  # type: ignore[arg-type]
        assert isinstance(adapter, SQLAdapter)
        try:
            creds = dict(profile_obj.credentials.connection_info())
        except:  # noqa: E722
            creds = {}

        yield DbtAdapter(adapter, creds)
