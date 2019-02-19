import traceback
from mock import patch
from unittest import TestCase

import logging
import dvc.logger as logger
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException
from dvc.utils.compat import StringIO


class TestLogger(TestCase):
    handlers = logger.logger.handlers
    color_patch = patch.object(logger, "colorize", new=lambda x, color="": x)

    def setUp(self):
        logger.logger.handlers = [logger.logging.StreamHandler(), logger.logging.StreamHandler()]
        logger.logger.handlers[0].stream = StringIO()
        logger.logger.handlers[1].stream = StringIO()
        logger.set_default_level()
        self.color_patch.start()

    def tearDown(self):
        logger.logger.handlers = self.handlers
        logger.be_verbose()
        self.color_patch.stop()

    @property
    def stdout(self):
        return logger.logger.handlers[0].stream.getvalue()

    @property
    def stderr(self):
        return logger.logger.handlers[1].stream.getvalue()

    def test_info(self):
        logger.info("message")

        self.assertEqual(self.stdout, "message\n")

    def test_debug(self):
        with logger.verbose():
            logger.debug("message")

        self.assertEqual(self.stdout, "Debug: message\n")

    def test_warning(self):
        logger.warning("message")

        self.assertEqual(self.stdout, "Warning: message\n")

    def test_error(self):
        logger.error("message")

        output = (
            "Error: message\n"
            "\n"
            "Having any troubles? Hit us up at https://dvc.org/support,"
            " we are always happy to help!\n"
        )

        self.assertEqual(self.stderr, output)

    def test_error_with_exception(self):
        try:
            raise Exception("exception description")
        except Exception:
            logger.error("")

        output = (
            "Error: exception description\n"
            "\n"
            "Having any troubles? Hit us up at https://dvc.org/support,"
            " we are always happy to help!\n"
        )

        self.assertEqual(self.stderr, output)

    def test_error_with_exception_and_message(self):
        try:
            raise Exception("exception description")
        except Exception:
            logger.error("message")

        output = (
            "Error: message - exception description\n"
            "\n"
            "Having any troubles? Hit us up at https://dvc.org/support,"
            " we are always happy to help!\n"
        )

        self.assertEqual(self.stderr, output)

    def test_error_with_chained_exception_and_message(self):
        try:
            raise DvcException("exception", cause=Exception("cause"))
        except Exception:
            logger.error("message")

        output = (
            "Error: message - exception: cause\n"
            "\n"
            "Having any troubles? Hit us up at https://dvc.org/support,"
            " we are always happy to help!\n"
        )

        self.assertEqual(self.stderr, output)

    def test_traceback(self):
        stack_trace1 = "stack_trace1\n"
        stack_trace2 = "stack_trace2\n"
        try:
            exc1 = Exception("exception1")
            exc2 = DvcException("exception2", cause=exc1)
            exc2.cause_tb = stack_trace1
            exc3 = DvcException("exception3", cause=exc2)
            exc3.cause_tb = stack_trace2
            raise exc3
        except Exception:
            stack_trace3 = traceback.format_exc()
            with logger.verbose():
                logger.error("message")

        output = (
            "Error: message - exception3: exception2: exception1\n"
            "{line}\n"
            "{stack_trace1}"
            "\n"
            "{stack_trace2}"
            "\n"
            "{stack_trace3}"
            "{line}\n"
            "\n"
            "Having any troubles? Hit us up at https://dvc.org/support,"
            " we are always happy to help!\n"
        ).format(
            line="-" * 60,
            stack_trace1=stack_trace1,
            stack_trace2=stack_trace2,
            stack_trace3=stack_trace3,
        )

        self.assertEqual(self.stderr, output)

    def test_box(self):
        logger.box("message")

        output = (
            "+-----------------+\n"
            "|                 |\n"
            "|     message     |\n"
            "|                 |\n"
            "+-----------------+\n"
            "\n"
        )

        self.assertEqual(self.stdout, output)

    def test_level(self):
        self.assertEqual(logger.level(), logging.INFO)

    def test_is_verbose(self):
        self.assertFalse(logger.is_verbose())

        with logger.verbose():
            self.assertTrue(logger.is_verbose())

    def test_is_quiet(self):
        self.assertFalse(logger.is_quiet())

        with logger.quiet():
            self.assertFalse(logger.is_verbose())

    def test_cli(self):
        class A(object):
            quiet = True
            verbose = False

        args = A()
        CmdBase.set_loglevel(args)
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.CRITICAL)

        args.quiet = False
        args.verbose = True
        CmdBase.set_loglevel(args)
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.DEBUG)
