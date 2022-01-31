def get_plot_id(plot_id: str, config_file_path: str = ""):
    return f"{config_file_path}::{plot_id}" if config_file_path else plot_id
