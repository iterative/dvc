import os

import pytest

from .dir_helpers import *  # noqa, pylint:disable=wildcard-import
from .remotes import *  # noqa, pylint:disable=wildcard-import

# Prevent updater and analytics from running their processes
os.environ["DVC_TEST"] = "true"
# Ensure progress output even when not outputting to raw sys.stderr console
os.environ["DVC_IGNORE_ISATTY"] = "true"


@pytest.fixture(autouse=True)
def reset_loglevel(request, caplog):
    """
    Use it to ensure log level at the start of each test
    regardless of dvc.logger.setup(), Repo configs or whatever.
    """
    level = request.config.getoption("--log-level")
    if level:
        with caplog.at_level(level.upper(), logger="dvc"):
            yield
    else:
        yield


@pytest.fixture(scope="session", autouse=True)
def _close_pools():
    from dvc.remote.pool import close_pools

    yield
    close_pools()
