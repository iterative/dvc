def get_plot_id(config_plot_id: str, config_file_path: str = ""):
    return (
        f"{config_file_path}::{config_plot_id}" if config_file_path else config_plot_id
    )
