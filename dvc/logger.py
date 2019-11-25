"""Manages logging configuration for dvc repo."""
from __future__ import unicode_literals

import logging.config
import logging.handlers

import colorama

from dvc.progress import Tqdm
from dvc.utils.compat import RecursionError
from dvc.utils.compat import str
from dvc.utils.compat import StringIO


FOOTER = (
    "\n{yellow}Having any troubles?{nc}"
    " Hit us up at {blue}https://dvc.org/support{nc},"
    " we are always happy to help!"
).format(
    blue=colorama.Fore.BLUE,
    nc=colorama.Fore.RESET,
    yellow=colorama.Fore.YELLOW,
)


class LoggingException(Exception):
    def __init__(self, record):
        msg = "failed to log {}".format(str(record))
        super(LoggingException, self).__init__(msg)


class ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.WARNING


class ExcludeInfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.INFO


class ColorFormatter(logging.Formatter):
    """Enable color support when logging to a terminal that supports it.

    Color support on Windows versions that do not support ANSI color codes is
    enabled by use of the colorama__ library.
    See the colorama documentation for details.

    __ https://pypi.python.org/pypi/colorama

    For records containing `exc_info`, it will use a custom `_walk_exc` to
    retrieve the whole traceback.
    """

    color_code = {
        "DEBUG": colorama.Fore.BLUE,
        "INFO": "",
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "CRITICAL": colorama.Fore.RED,
    }

    def format(self, record):
        if record.levelname == "INFO":
            return record.msg

        if record.levelname == "ERROR" or record.levelname == "CRITICAL":
            exception, stack_trace = self._parse_exc(record.exc_info)

            return (
                "{color}{levelname}{nc}: {description}" "{stack_trace}\n"
            ).format(
                color=self.color_code.get(record.levelname, ""),
                nc=colorama.Fore.RESET,
                levelname=record.levelname,
                description=self._description(record.msg, exception),
                msg=record.msg,
                stack_trace=stack_trace,
            )

        return "{color}{levelname}{nc}: {msg}".format(
            color=self.color_code.get(record.levelname, ""),
            nc=colorama.Fore.RESET,
            levelname=record.levelname,
            msg=record.msg,
        )

    def _current_level(self):
        return logging.getLogger("dvc").getEffectiveLevel()

    def _is_visible(self, record):
        return record.levelno >= self._current_level()

    def _description(self, message, exception):
        description = ""

        if exception and message:
            description = "{message} - {exception}"
        elif exception:
            description = "{exception}"
        elif message:
            description = "{message}"

        return description.format(message=message, exception=exception)

    def _walk_exc(self, exc_info):
        import traceback

        buffer = StringIO()

        traceback.print_exception(*exc_info, file=buffer)

        exc = exc_info[1]
        tb = buffer.getvalue()

        exc_list = [str(exc)]
        tb_list = [tb]

        # NOTE: parsing chained exceptions. See dvc/exceptions.py for more info
        while hasattr(exc, "cause") and exc.cause:
            exc_list.append(str(exc.cause))
            if hasattr(exc, "cause_tb") and exc.cause_tb:
                tb_list.insert(0, str(exc.cause_tb))
            exc = exc.cause

        return exc_list, tb_list

    def _parse_exc(self, exc_info):
        if not exc_info:
            return (None, "")

        exc_list, tb_list = self._walk_exc(exc_info)

        exception = ": ".join(exc_list)

        if self._current_level() == logging.DEBUG:
            stack_trace = (
                "\n" "{red}{line}{nc}\n" "{stack_trace}" "{red}{line}{nc}"
            ).format(
                red=colorama.Fore.RED,
                nc=colorama.Fore.RESET,
                line="-" * 60,
                stack_trace="\n".join(tb_list),
            )
        else:
            stack_trace = ""

        return (exception, stack_trace)


class LoggerHandler(logging.StreamHandler):
    def handleError(self, record):
        super(LoggerHandler, self).handleError(record)
        raise LoggingException(record)

    def emit(self, record):
        """Write to Tqdm's stream so as to not break progress-bars"""
        try:
            msg = self.format(record)
            Tqdm.write(
                msg, file=self.stream, end=getattr(self, "terminator", "\n")
            )
            self.flush()
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


def setup(level=logging.INFO):
    colorama.init()

    logging.config.dictConfig(
        {
            "version": 1,
            "filters": {
                "exclude_errors": {"()": ExcludeErrorsFilter},
                "exclude_info": {"()": ExcludeInfoFilter},
            },
            "formatters": {"color": {"()": ColorFormatter}},
            "handlers": {
                "console_info": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "INFO",
                    "formatter": "color",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_errors"],
                },
                "console_debug": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "DEBUG",
                    "formatter": "color",
                    "stream": "ext://sys.stdout",
                    "filters": ["exclude_info"],
                },
                "console_errors": {
                    "class": "dvc.logger.LoggerHandler",
                    "level": "WARNING",
                    "formatter": "color",
                    "stream": "ext://sys.stderr",
                },
            },
            "loggers": {
                "dvc": {
                    "level": level,
                    "handlers": [
                        "console_info",
                        "console_debug",
                        "console_errors",
                    ],
                },
                "paramiko": {
                    "level": logging.CRITICAL,
                    "handlers": [
                        "console_info",
                        "console_debug",
                        "console_errors",
                    ],
                },
                "flufl.lock": {
                    "level": logging.CRITICAL,
                    "handlers": [
                        "console_info",
                        "console_debug",
                        "console_errors",
                    ],
                },
            },
        }
    )
