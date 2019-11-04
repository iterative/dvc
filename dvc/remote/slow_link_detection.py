import logging
import sys
import time
from functools import wraps

import colorama

from dvc.config import Config


logger = logging.getLogger(__name__)
this = sys.modules[__name__]

this.timeout_seconds = 10.0
this.already_displayed = False
this.message = (
    "You can cut execution time considerably by using a different "
    "`cache.type` configuration.\n"
    "See {blue}https://dvc.org/doc/commands-reference/config#cache{reset} "
    "for more information.\n"
    "To disable this message, run:\n"
    "`dvc config cache.slow_link_warning false`".format(
        blue=colorama.Fore.BLUE, reset=colorama.Fore.RESET
    )
)


def slow_link_guard(f):
    @wraps(f)
    def wrapper(remote, *args, **kwargs):
        if this.already_displayed:
            return f(remote, *args, **kwargs)

        config = remote.repo.config.config.get(Config.SECTION_CACHE, {})
        cache_type = config.get(Config.SECTION_CACHE_TYPE)
        should_warn = config.get(Config.SECTION_CACHE_SLOW_LINK_WARNING, True)

        if not should_warn or cache_type:
            return f(remote, *args, **kwargs)

        start = time.time()
        result = f(remote, *args, **kwargs)
        delta = time.time() - start

        if delta >= this.timeout_seconds:
            logger.warning(this.message)
            this.already_displayed = True

        return result

    return wrapper
