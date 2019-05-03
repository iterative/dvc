import logging
import time

import colorama
from dvc.config import Config

logger = logging.getLogger(__name__)


class SlowLinkDetectorDecorator(object):
    LINKING_TIMEOUT_SECONDS = 10.0
    was_displayed = False

    @classmethod
    def should_display(cls):
        if not cls.was_displayed:
            cls.was_displayed = True
            return True
        return False

    def __init__(self, method):
        self.method = method

    def __call__(self, *args, **kwargs):
        start = time.time()
        result = self.method(*args, **kwargs)
        execution_time_seconds = time.time() - start

        if (
            execution_time_seconds >= self.LINKING_TIMEOUT_SECONDS
            and self.should_display()
        ):
            msg = (
                "You can cut execution time considerably. Check:\n"
                "{blue}https://dvc.org/doc/commands-reference/config#cache{"
                "reset}"
                "\nfor "
                "more information.\nTo disable this message, run:\n'dvc "
                "config "
                "cache.slow_link_warning False'".format(
                    blue=colorama.Fore.BLUE, reset=colorama.Fore.RESET
                )
            )
            logger.warning(msg)

        return result


def slow_link_guard(method):
    def call(remote_local, *args, **kwargs):
        cache_config = remote_local.repo.config.config.get(
            Config.SECTION_CACHE
        )
        should_warn = cache_config.get(
            Config.SECTION_CACHE_SLOW_LINK_WARNING, True
        ) and not cache_config.get(Config.SECTION_CACHE_TYPE, None)

        if should_warn:
            decorated = SlowLinkDetectorDecorator(method)
            return decorated(remote_local, *args, **kwargs)
        return method(remote_local, *args, **kwargs)

    return call
