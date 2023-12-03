from typing import Any, ContextManager, Optional, Union

from . import dbt_query, sqla
from .dbt_models import get_model
from .serializer import export

Adapter = Union[sqla.SQLAlchemyAdapter, dbt_query.DbtAdapter]


def get_adapter(
    config,
    project_dir: Optional[str] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
    **kwargs: Any,
) -> "ContextManager[Adapter]":
    if config:
        return sqla.adapter(config, **kwargs)
    return dbt_query.adapter(project_dir=project_dir, profile=profile, target=target)


__all__ = ["export", "get_adapter", "get_model", "Adapter"]
