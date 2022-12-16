from dvc.render import REVISION_FIELD, SRC_FIELD
from dvc.render.converter.image import ImageConverter


def test_image_converter_no_out():
    data = {"image.png": b"content"}
    converter = ImageConverter("image.png", data)
    datapoints, _ = converter.flat_datapoints("r")

    assert datapoints[0] == {
        REVISION_FIELD: "r",
        "filename": "image.png",
        SRC_FIELD: converter._encode_image(b"content"),
    }


def test_image_converter_with_out(tmp_dir):
    data = {"image.png": b"content"}
    converter = ImageConverter("image.png", data, {"out": tmp_dir / "foo"})

    datapoints, _ = converter.flat_datapoints("r")

    assert datapoints[0] == {
        REVISION_FIELD: "r",
        "filename": "image.png",
        SRC_FIELD: str(tmp_dir / "foo" / "r_image.png"),
    }

    assert (tmp_dir / "foo" / "r_image.png").read_bytes() == b"content"


def test_image_converter_with_slash_in_revision(tmp_dir):
    """Regression test for #7934"""
    data = {"image.png": b"content"}
    converter = ImageConverter("image.png", data, {"out": tmp_dir / "foo"})

    datapoints, _ = converter.flat_datapoints("feature/r")

    assert datapoints[0] == {
        REVISION_FIELD: "feature/r",
        "filename": "image.png",
        SRC_FIELD: str(tmp_dir / "foo" / "feature_r_image.png"),
    }

    assert (tmp_dir / "foo" / "feature_r_image.png").read_bytes() == b"content"
