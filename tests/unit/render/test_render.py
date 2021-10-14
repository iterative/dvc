import os

from dvc.render.utils import find_vega, render


def assert_website_has_image(page_path, revision, filename, image_content):
    index_path = page_path / "index.html"
    assert index_path.is_file()
    index_content = index_path.read_text()

    resources_filename = f"{revision}_{filename.replace(os.sep, '_')}"
    image_path = page_path / "static" / resources_filename
    assert image_path.is_file()

    img_html = f'<img src="{os.path.join("static", resources_filename)}">'
    assert img_html in index_content
    with open(image_path, "rb") as fobj:
        assert fobj.read() == image_content


def test_render(tmp_dir, dvc):
    data = {
        "HEAD": {
            "data": {
                "file.json": {
                    "data": [{"y": 5}, {"y": 6}],
                    "props": {"fields": {"y"}},
                },
                os.path.join("sub", "other_file.jpg"): {"data": b"content"},
            }
        },
        "v2": {
            "data": {
                "file.json": {
                    "data": [{"y": 3}, {"y": 5}],
                    "props": {"fields": {"y"}},
                },
                "other_file.jpg": {"data": b"content2"},
            }
        },
        "v1": {
            "data": {
                "some.csv": {
                    "data": [{"y": 2}, {"y": 3}],
                    "props": {"fields": {"y"}},
                },
                "another.gif": {"data": b"content3"},
            }
        },
    }

    render(dvc, data, path=os.path.join("results", "dir"))
    page_path = tmp_dir / "results" / "dir"
    index_path = page_path / "index.html"

    assert index_path.is_file()
    assert_website_has_image(
        page_path, "HEAD", os.path.join("sub", "other_file.jpg"), b"content"
    )
    assert_website_has_image(page_path, "v2", "other_file.jpg", b"content2")
    assert_website_has_image(page_path, "v1", "another.gif", b"content3")

    index_content = index_path.read_text()
    file_vega = find_vega(dvc, data, "file.json")
    some_vega = find_vega(dvc, data, "some.csv")

    def clean(txt: str) -> str:
        return txt.replace("\n", "").replace("\r", "").replace(" ", "")

    assert clean(file_vega) in clean(index_content)
    assert clean(some_vega) in clean(index_content)
