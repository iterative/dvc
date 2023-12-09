from typing import Any, ContextManager, Optional, Union

from . import dbt_query, sqla
from .dbt_models import get_model
from .serializer import export

Client = Union[sqla.SQLAlchemyClient, dbt_query.DbtClient]


def get_client(
    config,
    project_dir: Optional[str] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
    **kwargs: Any,
) -> "ContextManager[Client]":
    if config:
        return sqla.client(config, **kwargs)
    return dbt_query.client(project_dir=project_dir, profile=profile, target=target)


__all__ = ["export", "get_client", "get_model", "Client"]
