from dvc.utils.plots import get_plot_id, group_definitions_by_id


def test_get_plot_id():
    assert get_plot_id("plot_id", "config_path") == "config_path::plot_id"
    assert get_plot_id("plot_id", "") == "plot_id"


def test_group_definitions_by_id():
    definitions = {
        "config1": {"data": {"plot1": "definition1", "plot2": "definition2"}},
        "config2": {"data": {"plot1": "definition1"}},
    }
    assert group_definitions_by_id(definitions) == {
        "config1::plot1": ("plot1", "definition1"),
        "config2::plot1": ("plot1", "definition1"),
        "plot2": ("plot2", "definition2"),
    }
