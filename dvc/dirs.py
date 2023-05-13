import os

import platformdirs

APPNAME = "dvc"
APPAUTHOR = "iterative"

DVC_SYSTEM_CONFIG_DIR = "DVC_GLOBAL_CONFIG_DIR"
DVC_GLOBAL_CONFIG_DIR = "DVC_GLOBAL_CONFIG_DIR"
DVC_SITE_CACHE_DIR = "DVC_SITE_CACHE_DIR"


def system_config_dir():
    return os.getenv(DVC_SYSTEM_CONFIG_DIR) or platformdirs.site_config_dir(
        APPNAME, APPAUTHOR
    )


def global_config_dir():
    return os.getenv(DVC_GLOBAL_CONFIG_DIR) or platformdirs.user_config_dir(
        APPNAME, APPAUTHOR
    )


def site_cache_dir():
    return os.getenv(DVC_SITE_CACHE_DIR) or platformdirs.site_cache_dir(
        APPNAME, APPAUTHOR, opinion=True
    )
