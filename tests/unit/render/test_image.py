import os

import pytest

from dvc.render.image import ImageRenderer


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


def test_render(tmp_dir):
    data = {"workspace": {"data": {"file.jpg": {"data": b"content"}}}}

    page_dir = os.path.join("some", "path")
    html = ImageRenderer(data).generate_html(page_dir)

    assert (tmp_dir / page_dir).is_dir()
    image_file = tmp_dir / page_dir / "static" / "workspace_file.jpg"
    assert image_file.is_file()

    with open(image_file, "rb") as fobj:
        assert fobj.read() == b"content"

    assert "<p>file.jpg</p>" in html
    assert (
        f'<img src="{os.path.join("static", "workspace_file.jpg")}">' in html
    )
