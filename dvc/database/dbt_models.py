from typing import List, Optional

from dvc.log import logger

from .dbt_runner import dbt_show
from .dbt_utils import check_dbt
from .serializer import AgateSerializer

logger = logger.getChild(__name__)


def ref(
    name: str,
    package: Optional[str] = None,
    version: Optional[int] = None,
) -> str:
    parts: List[str] = []
    if package:
        parts.append(repr(package))

    parts.append(repr(name))
    if version:
        parts.append(f"{version=}")

    inner = ",".join(parts)
    return "{{ ref(" + inner + ") }}"


@check_dbt("model")
def get_model(
    name: str,
    package: Optional[str] = None,
    version: Optional[int] = None,
    profile: Optional[str] = None,
    target: Optional[str] = None,
) -> AgateSerializer:
    model = ref(name, package, version=version)
    q = f"select * from {model}"  # noqa: S608
    table = dbt_show(inline=q, profile=profile, target=target)
    return AgateSerializer(table)
