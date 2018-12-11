from mock import patch

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

import logging

from dvc.command.base import CmdBase
from dvc.logger import Logger, logger

from tests.basic_env import TestDvc


class StringContaining(str):
    def __eq__(self, other):
        return self in other


class TestLogger(TestDvc):
    def test_config(self):
        logger.set_level()
        logger.set_level('debug')
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.DEBUG)

        logger.set_level('error')
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.ERROR)

    def test_set_level(self):
        logger.set_level()
        logger.set_level('debug')
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.DEBUG)

    def test_be_quiet(self):
        logger.set_level()
        logger.be_quiet()
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.CRITICAL)

    def test_be_verbose(self):
        logger.set_level()
        logger.be_verbose()
        self.assertEqual(logger.logger.getEffectiveLevel(), logging.DEBUG)

    def test_colorize(self):
        for name, color in logger.COLOR_MAP.items():
            msg = logger.colorize(name, name)
            # This is not a tty, so it should not colorize anything
            self.assertEqual(msg, name)

    def test_handlers(self):
        Logger()
        Logger()
        self.assertEqual(len(logger.logger.handlers), 2)

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_stderr(self, mock_stdout, mock_stderr):
        logger = Logger(force=True)
        error_message = 'error msg'
        logger.error(error_message)
        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('Error: {}\n\nHaving any troubles? '
                         'Hit us up at dvc.org/support, we '
                         'are always happy to help!\n'.format(error_message),
                         mock_stderr.getvalue())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_stdout(self, mock_stdout, mock_stderr):
        logger = Logger(force=True)
        non_error_message = 'non-error message'
        logger.info(non_error_message)
        self.assertEqual('', mock_stderr.getvalue())
        self.assertEqual('{}\n'.format(non_error_message),
                         mock_stdout.getvalue())


class TestLoggerException(TestDvc):
    def setUp(self):
        super(TestLoggerException, self).setUp()

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_empty_err_msg(self, mock_stdout, mock_stderr):
        from dvc.exceptions import DvcException

        logger = Logger(force=True)

        exc_msg = 'msg'
        logger.error("", exc=DvcException(exc_msg))

        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('Error: {}\n\nHaving any troubles? '
                         'Hit us up at dvc.org/support, we '
                         'are always happy to help!\n'.format(exc_msg),
                         mock_stderr.getvalue())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_empty_exc_msg(self, mock_stdout, mock_stderr):
        from dvc.exceptions import DvcException

        logger = Logger(force=True)

        err_msg = 'msg'
        logger.error(err_msg, exc=DvcException(""))

        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('Error: {}\n\nHaving any troubles? '
                         'Hit us up at dvc.org/support, we '
                         'are always happy to help!\n'.format(err_msg),
                         mock_stderr.getvalue())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_msg_and_exc(self, mock_stdout, mock_stderr):
        from dvc.exceptions import DvcException

        logger = Logger(force=True)

        err_msg = 'error'
        exc_msg = 'exception'
        logger.error(err_msg, exc=DvcException(exc_msg))

        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('Error: {}: {}\n\nHaving any troubles? '
                         'Hit us up at dvc.org/support, we are always '
                         'happy to help!\n'.format(err_msg, exc_msg),
                         mock_stderr.getvalue())


class TestLoggerQuiet(TestDvc):
    def setUp(self):
        super(TestLoggerQuiet, self).setUp()

        class A(object):
            quiet = True
            verbose = False

        CmdBase._set_loglevel(A())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_progress(self, mock_stdout, mock_stderr):
        from dvc.progress import progress
        progress.update_target('target', 50, 100)
        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('', mock_stderr.getvalue())

    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.stdout', new_callable=StringIO)
    def test_box(self, mock_stdout, mock_stderr):
        logger.box('message')
        self.assertEqual('', mock_stdout.getvalue())
        self.assertEqual('', mock_stderr.getvalue())


class TestLoggerCLI(TestDvc):
    def test(self):
        from dvc.logger import logger

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
