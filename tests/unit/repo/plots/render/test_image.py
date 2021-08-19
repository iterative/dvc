import pytest

from dvc.repo.plots.render import ImageRenderer


@pytest.mark.parametrize(
    "extension, matches",
    (
        (".csv", False),
        (".json", False),
        (".tsv", False),
        (".yaml", False),
        (".jpg", True),
        (".gif", True),
        (".jpeg", True),
        (".png", True),
    ),
)
def test_matches(extension, matches):
    filename = "file" + extension
    data = {
        "HEAD": {"data": {filename: {}}},
        "v1": {"data": {filename: {}}},
    }
    assert ImageRenderer.matches(data) == matches
