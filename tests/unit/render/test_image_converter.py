from dvc.render import REVISION_FIELD, SRC_FIELD
from dvc.render.image_converter import ImageConverter


def test_image_converter_no_out():
    data = b"content"
    converter = ImageConverter()

    datapoints, _ = converter.convert(data, "r", "image.png")

    assert datapoints[0] == {
        REVISION_FIELD: "r",
        "filename": "image.png",
        SRC_FIELD: converter._encode_image(b"content"),
    }


def test_image_converter_with_out(tmp_dir):
    data = b"content"
    converter = ImageConverter({"out": tmp_dir / "foo"})

    datapoints, _ = converter.convert(data, "r", "image.png")

    assert datapoints[0] == {
        REVISION_FIELD: "r",
        "filename": "image.png",
        SRC_FIELD: str(tmp_dir / "foo" / "r_image.png"),
    }

    assert (tmp_dir / "foo" / "r_image.png").read_bytes() == b"content"
