import pytest

from dvc.annotations import Annotation


@pytest.mark.parametrize(
    "kwargs",
    [
        {"desc": "desc", "type": "type", "labels": ["label1", "label2"]},
        {"desc": "desc", "type": "type", "meta": {"key": "value"}},
    ],
)
def test_annotation_to_dict(kwargs):
    annot = Annotation(**kwargs)
    assert annot.to_dict() == kwargs
