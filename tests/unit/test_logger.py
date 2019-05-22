from __future__ import unicode_literals

import traceback
import logging
import colorama

import dvc.logger
from dvc.exceptions import DvcException


logger = logging.getLogger("dvc")
formatter = dvc.logger.ColorFormatter()
colors = {
    "blue": colorama.Fore.BLUE,
    "red": colorama.Fore.RED,
    "yellow": colorama.Fore.YELLOW,
    "nc": colorama.Fore.RESET,
}


class TestColorFormatter:
    def test_debug(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            logger.debug("message")

            expected = "{blue}DEBUG{nc}: message".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_info(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.info("message")

            assert "message" == formatter.format(caplog.records[0])

    def test_warning(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.warning("message")

            expected = "{yellow}WARNING{nc}: message".format(**colors)

            assert expected == formatter.format(caplog.records[0])

    def test_error(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            logger.error("message")

            expected = (
                "{red}ERROR{nc}: message\n"
                "\n"
                "{footer}".format(footer=formatter.footer, **colors)
            )

            assert expected == formatter.format(caplog.records[0])

    def test_exception(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise ValueError
            except Exception:
                logger.exception("message")

            expected = (
                "{red}ERROR{nc}: message\n"
                "\n"
                "{footer}".format(footer=formatter.footer, **colors)
            )

            assert expected == formatter.format(caplog.records[0])

    def test_exception_with_description_and_without_message(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                logger.exception("")

            expected = (
                "{red}ERROR{nc}: description\n"
                "\n"
                "{footer}".format(footer=formatter.footer, **colors)
            )

            assert expected == formatter.format(caplog.records[0])

    def test_exception_with_description_and_message(self, caplog):
        with caplog.at_level(logging.INFO, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                logger.exception("message")

            expected = (
                "{red}ERROR{nc}: message - description\n"
                "\n"
                "{footer}".format(footer=formatter.footer, **colors)
            )

            assert expected == formatter.format(caplog.records[0])

    def test_exception_under_verbose(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("description")
            except Exception:
                stack_trace = traceback.format_exc()
                logger.exception("")

            expected = (
                "{red}ERROR{nc}: description\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}\n"
                "\n"
                "{footer}".format(
                    footer=formatter.footer,
                    line="-" * 60,
                    stack_trace=stack_trace,
                    **colors
                )
            )

            assert expected == formatter.format(caplog.records[0])

    def test_nested_exceptions(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="dvc"):
            try:
                raise Exception("first")
            except Exception as exc:
                first_traceback = traceback.format_exc()
                try:
                    raise DvcException("second", cause=exc)
                except DvcException:
                    second_traceback = traceback.format_exc()
                    logger.exception("message")

            expected = (
                "{red}ERROR{nc}: message - second: first\n"
                "{red}{line}{nc}\n"
                "{stack_trace}"
                "{red}{line}{nc}\n"
                "\n"
                "{footer}".format(
                    footer=formatter.footer,
                    line="-" * 60,
                    stack_trace="\n".join([first_traceback, second_traceback]),
                    **colors
                )
            )

            assert expected == formatter.format(caplog.records[0])

    def test_progress_awareness(self, mock, capsys, caplog):
        from dvc.progress import progress

        with mock.patch("sys.stdout.isatty", return_value=True):
            progress.set_n_total(100)
            progress.update_target("progress", 1, 10)

            # logging an invisible message should not break
            # the progress bar output
            with caplog.at_level(logging.INFO, logger="dvc"):
                debug_record = logging.LogRecord(
                    name="dvc",
                    level=logging.DEBUG,
                    pathname=__name__,
                    lineno=1,
                    msg="debug",
                    args=(),
                    exc_info=None,
                )

                formatter.format(debug_record)
                captured = capsys.readouterr()
                assert "\n" not in captured.out

            # just when the message is actually visible
            with caplog.at_level(logging.INFO, logger="dvc"):
                logger.info("some info")
                captured = capsys.readouterr()
                assert "\n" in captured.out
