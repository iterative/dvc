import pytest
from funcy import merge

from tests.utils.plots import get_plot


@pytest.mark.studio
def test_api(tmp_dir, dvc, repo_with_plots):
    repo_state = repo_with_plots()
    image_v1, linear_v1, confusion_v1, confusion_params = next(repo_state)

    workspace_data = next(dvc.plots.collect())

    assert get_plot(workspace_data, "workspace", file="image.png", endkey="props") == {}
    image_source = get_plot(
        workspace_data, "workspace", file="image.png", endkey="data_source"
    )
    assert callable(image_source)
    assert image_source() == {"data": image_v1}

    assert get_plot(
        workspace_data, "workspace", file="linear.json", endkey="props"
    ) == {"title": "linear", "x": "x"}
    linear_source = get_plot(
        workspace_data, "workspace", file="linear.json", endkey="data_source"
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v1}

    assert (
        get_plot(workspace_data, "workspace", file="confusion.json", endkey="props")
        == confusion_params
    )
    confusion_source = get_plot(
        workspace_data,
        "workspace",
        file="confusion.json",
        endkey="data_source",
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v1}

    image_v2, linear_v2, confusion_v2, _ = next(repo_state)
    data_generator = dvc.plots.collect(revs=["workspace", "HEAD"])

    workspace_data = next(data_generator)

    assert get_plot(workspace_data, "workspace", file="image.png", endkey="props") == {}
    image_source = get_plot(
        workspace_data, "workspace", file="image.png", endkey="data_source"
    )
    assert callable(image_source)
    assert image_source() == {"data": image_v2}

    assert get_plot(
        workspace_data, "workspace", file="linear.json", endkey="props"
    ) == {"title": "linear", "x": "x"}
    linear_source = get_plot(
        workspace_data, "workspace", file="linear.json", endkey="data_source"
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v2}

    assert (
        get_plot(workspace_data, "workspace", file="confusion.json", endkey="props")
        == confusion_params
    )
    confusion_source = get_plot(
        workspace_data,
        "workspace",
        file="confusion.json",
        endkey="data_source",
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v2}

    head_data = next(data_generator)

    assert get_plot(head_data, "HEAD", file="image.png", endkey="props") == {}
    image_source = get_plot(head_data, "HEAD", file="image.png", endkey="data_source")
    assert callable(image_source)
    assert image_source() == {"data": image_v1}

    assert get_plot(head_data, "HEAD", file="linear.json", endkey="props") == {
        "title": "linear",
        "x": "x",
    }
    linear_source = get_plot(
        head_data, "HEAD", file="linear.json", endkey="data_source"
    )
    assert callable(linear_source)
    assert linear_source() == {"data": linear_v1}

    assert (
        get_plot(head_data, "HEAD", file="confusion.json", endkey="props")
        == confusion_params
    )
    confusion_source = get_plot(
        head_data, "HEAD", file="confusion.json", endkey="data_source"
    )
    assert callable(confusion_source)
    assert confusion_source() == {"data": confusion_v1}


@pytest.mark.studio
def test_api_with_config_plots(tmp_dir, dvc, capsys, repo_with_config_plots):
    repo_state = repo_with_config_plots()
    plots_state = next(repo_state)

    plots_data = next(dvc.plots.collect())

    assert get_plot(
        plots_data, "workspace", typ="definitions", file="dvc.yaml"
    ) == merge(*plots_state["configs"]["dvc.yaml"])

    for file in plots_state["data"]:
        data_source = get_plot(plots_data, "workspace", file=file, endkey="data_source")
        assert callable(data_source)
        assert data_source() == {"data": plots_state["data"][file]}
