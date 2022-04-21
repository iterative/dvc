import dpath.util
import pytest


@pytest.mark.studio
def test_api(tmp_dir, dvc, repo_with_plots):
    repo_state = repo_with_plots()
    image_v1, linear_v1, confusion_v1, confusion_params = next(repo_state)

    workspace_data = next(dvc.plots.collect())

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "image.png", "props"]
        )
        == {}
    )
    image_source = dpath.util.get(
        workspace_data, ["workspace", "data", "image.png", "data_source"]
    )
    assert callable(image_source)
    assert image_source() == {"data": image_v1}

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "linear.json", "props"]
        )
        == {}
    )
    linear_source = dpath.util.get(
        workspace_data, ["workspace", "data", "linear.json", "data_source"]
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v1}

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "confusion.json", "props"]
        )
        == confusion_params
    )
    confusion_source = dpath.util.get(
        workspace_data, ["workspace", "data", "confusion.json", "data_source"]
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v1}

    image_v2, linear_v2, confusion_v2, _ = next(repo_state)
    data_generator = dvc.plots.collect(revs=["workspace", "HEAD"])

    workspace_data = next(data_generator)

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "image.png", "props"]
        )
        == {}
    )
    image_source = dpath.util.get(
        workspace_data, ["workspace", "data", "image.png", "data_source"]
    )
    assert callable(image_source)
    assert image_source() == {"data": image_v2}

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "linear.json", "props"]
        )
        == {}
    )
    linear_source = dpath.util.get(
        workspace_data, ["workspace", "data", "linear.json", "data_source"]
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v2}

    assert (
        dpath.util.get(
            workspace_data, ["workspace", "data", "confusion.json", "props"]
        )
        == confusion_params
    )
    confusion_source = dpath.util.get(
        workspace_data, ["workspace", "data", "confusion.json", "data_source"]
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v2}

    head_data = next(data_generator)

    assert (
        dpath.util.get(head_data, ["HEAD", "data", "image.png", "props"]) == {}
    )
    image_source = dpath.util.get(
        head_data, ["HEAD", "data", "image.png", "data_source"]
    )
    assert callable(image_source)
    assert image_source() == {"data": image_v1}

    assert (
        dpath.util.get(head_data, ["HEAD", "data", "linear.json", "props"])
        == {}
    )
    linear_source = dpath.util.get(
        head_data, ["HEAD", "data", "linear.json", "data_source"]
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v1}

    assert (
        dpath.util.get(head_data, ["HEAD", "data", "confusion.json", "props"])
        == confusion_params
    )
    confusion_source = dpath.util.get(
        head_data, ["HEAD", "data", "confusion.json", "data_source"]
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v1}
