"""Manages logging configuration for dvc repo."""

import traceback
import logging.config
import logging.handlers

import colorama

from dvc.progress import Tqdm


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
    pass


class ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.WARNING


class ExcludeInfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.INFO


class ColorFormatter(logging.Formatter):
    """Spit out colored text in supported terminals.

    colorama__ makes ANSI escape character sequences work under Windows.
    See the colorama documentation for details.

    __ https://pypi.python.org/pypi/colorama
    """

    color_code = {
        "DEBUG": colorama.Fore.BLUE,
        "INFO": "",
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "CRITICAL": colorama.Fore.RED,
    }

    def format(self, record):
        info = record.msg

        if record.levelname == "INFO":
            return info

        if record.exc_info:
            _, exception, _ = record.exc_info

            info = "{message}{separator}{exception}".format(
                message=record.msg or "",
                separator=" - " if record.msg and exception.args else "",
                exception=": ".join(self._causes(exception)),
            )

            if self._current_level() == logging.DEBUG:
                trace = "".join(traceback.format_exception(*record.exc_info))

                return (
                    "{red}{levelname}{nc}: {info}\n"
                    "{red}{line}{nc}\n"
                    "{trace}"
                    "{red}{line}{nc}".format(
                        levelname=record.levelname,
                        info=info,
                        red=colorama.Fore.RED,
                        line="-" * 60,
                        trace=trace,
                        nc=colorama.Fore.RESET,
                    )
                )

        return "{color}{levelname}{nc}: {info}".format(
            color=self.color_code[record.levelname],
            levelname=record.levelname,
            nc=colorama.Fore.RESET,
            info=info,
        )

    def _causes(self, exc):
        while exc:
            yield str(exc)
            exc = exc.__cause__

    def _current_level(self):
        return logging.getLogger("dvc").getEffectiveLevel()


class LoggerHandler(logging.StreamHandler):
    def handleError(self, record):
        super().handleError(record)
        raise LoggingException("failed to log {}".format(record))

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
