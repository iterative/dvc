import os
from typing import Optional

import platformdirs

from . import env

APPNAME = "dvc"
APPAUTHOR = "iterative"


def system_config_dir():
    return os.getenv(env.DVC_SYSTEM_CONFIG_DIR) or platformdirs.site_config_dir(
        APPNAME, APPAUTHOR
    )


def global_config_dir():
    return os.getenv(env.DVC_GLOBAL_CONFIG_DIR) or platformdirs.user_config_dir(
        APPNAME, APPAUTHOR
    )


def site_cache_dir(config_site_cache_dir: Optional[str] = None):
    from platformdirs import PlatformDirs
    from platformdirs.unix import Unix

    if dvc_site_cache_dir := os.getenv(env.DVC_SITE_CACHE_DIR):
        return dvc_site_cache_dir

    if config_site_cache_dir is not None:
        return config_site_cache_dir

    if issubclass(Unix, PlatformDirs):
        # Return the cache directory shared by users, e.g. `/var/tmp/$appname`
        # NOTE: platformdirs>=5 changed `site_cache_dir` to return /var/cache/$appname.
        # as the following path is considered insecure.
        # For details, see: https://github.com/tox-dev/platformdirs/pull/239

        # FIXME: keeping the old behavior temporarily to avoid dependency conflict.
        #        In the future, consider migrating to a more secure directory.
        return f"/var/tmp/{APPNAME}"  # noqa: S108

    return platformdirs.site_cache_dir(APPNAME, APPAUTHOR, opinion=True)
