"""Manages logging configuration for DVC repo."""

import logging
import logging.config
import logging.handlers
import os
import sys

import colorama

from dvc.progress import Tqdm


def add_logging_level(level_name, level_num, method_name=None):
    """
    Adds a new logging level to the `logging` module and the
    currently configured logging class.

    Uses the existing numeric level_num if already defined.

    Based on https://stackoverflow.com/questions/2183233
    """
    if method_name is None:
        method_name = level_name.lower()

    # If the level name is already defined as a top-level `logging`
    # constant, then adopt the existing numeric level.
    if hasattr(logging, level_name):
        existing_level_num = getattr(logging, level_name)
        assert isinstance(existing_level_num, int)
        level_num = existing_level_num

    def log_for_level(self, message, *args, **kwargs):
        if self.isEnabledFor(level_num):
            self._log(level_num, message, args, **kwargs)

    def log_to_root(message, *args, **kwargs):
        logging.log(level_num, message, *args, **kwargs)

    # getLevelName resolves the numeric log level if already defined,
    # otherwise returns a string
    if not isinstance(logging.getLevelName(level_name), int):
        logging.addLevelName(level_num, level_name)

    if not hasattr(logging, level_name):
        setattr(logging, level_name, level_num)

    if not hasattr(logging.getLoggerClass(), method_name):
        setattr(logging.getLoggerClass(), method_name, log_for_level)

    if not hasattr(logging, method_name):
        setattr(logging, method_name, log_to_root)


class LoggingException(Exception):
    def __init__(self, record):
        msg = f"failed to log {record!s}"
        super().__init__(msg)


def exclude_filter(level: int):
    def filter_fn(record: "logging.LogRecord") -> bool:
        return record.levelno < level

    return filter_fn


class ColorFormatter(logging.Formatter):
    """Spit out colored text in supported terminals.

    colorama__ makes ANSI escape character sequences work under Windows.
    See the colorama documentation for details.

    __ https://pypi.python.org/pypi/colorama

    If record has an extra `tb_only` attribute, it will not show the
    exception cause, just the message and the traceback.
    """

    reset = colorama.Fore.RESET
    color_codes = {
        "TRACE": colorama.Fore.GREEN,
        "DEBUG": colorama.Fore.BLUE,
        "WARNING": colorama.Fore.YELLOW,
        "ERROR": colorama.Fore.RED,
        "CRITICAL": colorama.Fore.RED,
    }

    def __init__(self, log_colors: bool = True, show_traceback: bool = False) -> None:
        super().__init__()
        self.log_colors = log_colors
        self.show_traceback = show_traceback

    def format(self, record) -> str:  # noqa: C901
        record.message = record.getMessage()
        msg = self.formatMessage(record)

        if record.levelno == logging.INFO:
            return msg

        ei = record.exc_info
        if ei:
            cause = ""
            if not getattr(record, "tb_only", False):
                cause = ": ".join(_iter_causes(ei[1]))
            sep = " - " if msg and cause else ""
            msg = msg + sep + cause

        asctime = ""
        verbose = _is_verbose()
        if verbose:
            asctime = self.formatTime(record, self.datefmt)
        if verbose or self.show_traceback:
            if ei and not record.exc_text:
                record.exc_text = self.formatException(ei)
            if record.exc_text:
                if msg[-1:] != "\n":
                    msg = msg + "\n"
                msg = msg + record.exc_text + "\n"
            if record.stack_info:
                if msg[-1:] != "\n":
                    msg = msg + "\n"
                msg = msg + self.formatStack(record.stack_info) + "\n"

        level = record.levelname
        if self.log_colors:
            color = self.color_codes[level]
            if asctime:
                asctime = color + asctime + self.reset
            level = color + level + self.reset
        return asctime + (" " if asctime else "") + level + ": " + msg


class LoggerHandler(logging.StreamHandler):
    def handleError(self, record):  # noqa: N802
        super().handleError(record)
        raise LoggingException(record)

    def emit_pretty_exception(self, exc, verbose: bool = False):
        return exc.__pretty_exc__(verbose=verbose)

    def emit(self, record):
        """Write to Tqdm's stream so as to not break progress-bars"""
        try:
            if record.exc_info:
                _, exc, *_ = record.exc_info
                if hasattr(exc, "__pretty_exc__"):
                    try:
                        self.emit_pretty_exception(exc, verbose=_is_verbose())
                        if not _is_verbose():
                            return
                    except Exception:  # noqa: BLE001, S110
                        pass

            msg = self.format(record)
            Tqdm.write(msg, file=self.stream, end=getattr(self, "terminator", "\n"))
            self.flush()
        except (BrokenPipeError, RecursionError):
            raise
        except Exception:  # noqa: BLE001
            self.handleError(record)


def _is_verbose():
    return (
        logging.NOTSET < logging.getLogger("dvc").getEffectiveLevel() <= logging.DEBUG
    )


def _iter_causes(exc):
    while exc:
        yield str(exc)
        exc = exc.__cause__


def set_loggers_level(level: int = logging.INFO) -> None:
    for name in ["dvc", "dvc_objects", "dvc_data"]:
        logging.getLogger(name).setLevel(level)


def setup(level: int = logging.INFO, log_colors: bool = True) -> None:
    colorama.init()

    color_out = log_colors and bool(sys.stdout) and sys.stdout.isatty()
    color_err = log_colors and bool(sys.stderr) and sys.stderr.isatty()

    formatter = ColorFormatter(log_colors=color_out)

    console_info = LoggerHandler(sys.stdout)
    console_info.setLevel(logging.INFO)
    console_info.setFormatter(formatter)
    console_info.addFilter(exclude_filter(logging.WARNING))

    console_debug = LoggerHandler(sys.stdout)
    console_debug.setLevel(logging.DEBUG)
    console_debug.setFormatter(formatter)
    console_debug.addFilter(exclude_filter(logging.INFO))

    add_logging_level("TRACE", logging.DEBUG - 5)

    console_trace = LoggerHandler(sys.stdout)
    console_trace.setLevel(logging.TRACE)  # type: ignore[attr-defined]
    console_trace.setFormatter(formatter)
    console_trace.addFilter(exclude_filter(logging.DEBUG))

    show_traceback = bool(os.environ.get("DVC_SHOW_TRACEBACK"))
    err_formatter = ColorFormatter(log_colors=color_err, show_traceback=show_traceback)
    console_errors = LoggerHandler(sys.stderr)
    console_errors.setLevel(logging.WARNING)
    console_errors.setFormatter(err_formatter)

    for name in ["dvc", "dvc_objects", "dvc_data"]:
        logger = logging.getLogger(name)
        logger.setLevel(level)
        for handler in [console_info, console_debug, console_trace, console_errors]:
            logger.addHandler(handler)

    if level >= logging.DEBUG:
        # Unclosed session errors for asyncio/aiohttp are only available
        # on the tracing mode for extensive debug purposes. They are really
        # noisy, and this is potentially somewhere in the client library
        # not managing their own session. Even though it is the best practice
        # for them to do so, we can be assured that these errors raised when
        # the object is getting deallocated, so no need to take any extensive
        # action.
        logging.getLogger("asyncio").setLevel(logging.CRITICAL)
        logging.getLogger("aiohttp").setLevel(logging.CRITICAL)
