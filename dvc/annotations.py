from dataclasses import asdict, dataclass, field, fields
from typing import Any, ClassVar, Dict, List, Optional

from funcy import compact
from voluptuous import Required


@dataclass
class Annotation:
    PARAM_DESC: ClassVar[str] = "desc"
    PARAM_TYPE: ClassVar[str] = "type"
    PARAM_LABELS: ClassVar[str] = "labels"
    PARAM_META: ClassVar[str] = "meta"

    desc: Optional[str] = None
    type: Optional[str] = None
    labels: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, str]:
        return compact(asdict(self))


@dataclass
class Artifact:
    PARAM_PATH: ClassVar[str] = "path"
    PARAM_DESC: ClassVar[str] = "desc"
    PARAM_TYPE: ClassVar[str] = "type"
    PARAM_LABELS: ClassVar[str] = "labels"
    PARAM_META: ClassVar[str] = "meta"

    path: str
    desc: Optional[str] = None
    type: Optional[str] = None
    labels: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, str]:
        return compact(asdict(self))


ANNOTATION_FIELDS = [field.name for field in fields(Annotation)]
ANNOTATION_SCHEMA = {
    Annotation.PARAM_DESC: str,
    Annotation.PARAM_TYPE: str,
    Annotation.PARAM_LABELS: [str],
    Annotation.PARAM_META: object,
}
ARTIFACT_SCHEMA: Dict[Any, Any] = {
    Required(Artifact.PARAM_PATH): str,
    **ANNOTATION_SCHEMA,  # type: ignore[arg-type]
}
