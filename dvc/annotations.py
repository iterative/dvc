from dataclasses import asdict, dataclass, field, fields
from typing import ClassVar, Dict, List, Optional

from funcy import compact


@dataclass
class Annotation:
    PARAM_DESC: ClassVar[str] = "desc"
    PARAM_TYPE: ClassVar[str] = "type"
    PARAM_LABELS: ClassVar[str] = "labels"

    desc: Optional[str] = None
    type: Optional[str] = None
    labels: List[str] = field(default_factory=list)

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
}
