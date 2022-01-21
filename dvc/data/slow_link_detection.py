# type: ignore
import logging
import sys
import time
from functools import wraps

import colorama

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
    def wrapper(self, odb, *args, **kwargs):
        if this.already_displayed:
            return f(self, odb, *args, **kwargs)

        if not odb.slow_link_warning or odb.cache_types:
            return f(self, odb, *args, **kwargs)

        start = time.time()
        result = f(self, odb, *args, **kwargs)
        delta = time.time() - start

        if delta >= this.timeout_seconds:
            logger.warning(this.message)
            this.already_displayed = True

        return result

    return wrapper
