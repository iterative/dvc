from collections import defaultdict


def get_plot_id(config_plot_id: str, config_file_path: str = ""):
    return (
        f"{config_file_path}::{config_plot_id}" if config_file_path else config_plot_id
    )


def group_definitions_by_id(
    definitions: dict[str, dict],
) -> dict[str, tuple[str, dict]]:
    """
    Format ID and extracts plot_definition for each plot.

    Arguments:
        definitions: dict of {config_file: config_file_content}.

    Returns:
        Dict of {plot_id: (original_plot_id, plot_definition)}.
    """
    groups_by_config: dict = defaultdict(dict)
    groups_by_id: dict = {}
    for config_file, config_file_content in definitions.items():
        for plot_id, plot_definition in config_file_content.get("data", {}).items():
            groups_by_config[plot_id][config_file] = (plot_id, plot_definition)
    # only keep config_file if the same plot_id is in multiple config_files
    for plot_id, configs in groups_by_config.items():
        if len(configs) == 1:
            groups_by_id[plot_id] = next(iter(configs.values()))
        else:
            for config_file, content in configs.items():
                full_id = get_plot_id(plot_id, config_file)
                groups_by_id[full_id] = content
    return groups_by_id
