import os

import platformdirs

from . import env

APPNAME = "dvc"
APPAUTHOR = "iterative"


def system_config_dir():
    return os.getenv(env.DVC_SYSTEM_CONFIG_DIR) or platformdirs.site_config_dir(
        APPNAME, APPAUTHOR
    )


def global_config_dir():
    return os.getenv(env.DVC_SYSTEM_CONFIG_DIR) or platformdirs.user_config_dir(
        APPNAME, APPAUTHOR
    )


def site_cache_dir():
    return os.getenv(env.DVC_SITE_CACHE_DIR) or platformdirs.site_cache_dir(
        APPNAME, APPAUTHOR, opinion=True
    )
