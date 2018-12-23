from mock import patch
from unittest import TestCase

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import logging
import dvc.logger as logger
from dvc.command.base import CmdBase


class TestLogger(TestCase):
    handlers = logger.logger.handlers
    color_patch = patch.object(logger, 'colorize', new=lambda x, color='': x)

    def setUp(self):
        logger.logger.handlers[0].stream = StringIO()
        logger.logger.handlers[1].stream = StringIO()
        logger._set_default_level()
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
        logger.info('message')

        self.assertEqual(self.stdout, 'message\n')

    def test_debug(self):
        with logger.verbose():
            logger.debug('message')

        self.assertEqual(self.stdout, 'Debug: message\n')

    def test_warning(self):
        logger.warning('message')

        self.assertEqual(self.stdout, 'Warning: message\n')

    def test_error(self):
        logger.error('message')

        output = (
            'Error: message\n'
            '\n'
            'Having any troubles? Hit us up at https://dvc.org/support,'
            ' we are always happy to help!\n'
        )

        self.assertEqual(self.stderr, output)

    def test_error_with_exception(self):
        try:
            raise Exception('exception description')
        except Exception:
            logger.error('')

        output = (
            'Error: exception description\n'
            '\n'
            'Having any troubles? Hit us up at https://dvc.org/support,'
            ' we are always happy to help!\n'
        )

        self.assertEqual(self.stderr, output)

    def test_error_with_exception_and_message(self):
        try:
            raise Exception('exception description')
        except Exception:
            logger.error('message')

        output = (
            'Error: message - exception description\n'
            '\n'
            'Having any troubles? Hit us up at https://dvc.org/support,'
            ' we are always happy to help!\n'
        )

        self.assertEqual(self.stderr, output)

    def test_box(self):
        logger.box('message')

        output = (
            '+-----------------+\n'
            '|                 |\n'
            '|     message     |\n'
            '|                 |\n'
            '+-----------------+\n'
            '\n'
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
        CmdBase._set_loglevel(args)
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.CRITICAL)

        args.quiet = False
        args.verbose = True
        CmdBase._set_loglevel(args)
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.DEBUG)
