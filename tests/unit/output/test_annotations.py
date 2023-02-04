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


def test_annotation_update():
    annot = Annotation(desc="desc", labels=["label1", "label2"])
    annot.update(labels=["label"], type="type", new="new", meta={"key": "value"})

    assert vars(annot) == {
        "desc": "desc",
        "type": "type",
        "labels": ["label"],
        "meta": {"key": "value"},
    }
