from dataclasses import asdict, dataclass, field, fields
from typing import Any, ClassVar, Dict, List, Optional

from funcy import compact


@dataclass
class Annotation:
    PARAM_DESC: ClassVar[str] = "desc"
    PARAM_TYPE: ClassVar[str] = "type"
    PARAM_LABELS: ClassVar[str] = "labels"
    PARAM_META: ClassVar[str] = "meta"
    # PARAM_PATH: ClassVar[str] = "path"

    desc: Optional[str] = None
    type: Optional[str] = None  # noqa: A003
    labels: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    # path: Optional[str] = None

    def update(self, **kwargs) -> "Annotation":
        for attr, value in kwargs.items():
            if value and hasattr(self, attr):
                setattr(self, attr, value)
        return self

    def to_dict(self) -> Dict[str, str]:
        return compact(asdict(self))


ANNOTATION_FIELDS = [field.name for field in fields(Annotation)]
ANNOTATION_SCHEMA = {
    Annotation.PARAM_DESC: str,
    Annotation.PARAM_TYPE: str,
    Annotation.PARAM_LABELS: [str],
    Annotation.PARAM_META: object,
    # Annotation.PARAM_PATH: str,
}
