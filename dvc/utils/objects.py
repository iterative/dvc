from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from functools import cached_property
else:
    from funcy import cached_property  # noqa: TID251

__all__ = ["cached_property"]
