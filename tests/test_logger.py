from mock import patch

import colorama
import logging

from dvc.command.base import CmdBase
from dvc.config import Config
from dvc.logger import Logger
from dvc.main import main

from tests.basic_env import TestDvc


class StringContaining(str):
    def __eq__(self, other):
        return self in other


class TestLogger(TestDvc):
    def test_config(self):
        Logger('debug')
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.DEBUG)

        Logger('error')
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.ERROR)

    def test_set_level(self):
        Logger.set_level('debug')
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.DEBUG)

    def test_be_quiet(self):
        Logger.be_quiet()
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.CRITICAL)

    def test_be_verbose(self):
        Logger.be_verbose()
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.DEBUG)

    def test_colorize(self):
        for name, color in Logger.COLOR_MAP.items():
            msg = Logger.colorize(name, name)
            # This is not a tty, so it should not colorize anything
            self.assertEqual(msg, name)

    @patch('sys.stderr.write')
    @patch('sys.stdout.write')
    def test_stderr(self, mock_stdout, mock_stderr):
        error_message = 'error msg'
        Logger.init()
        Logger.error(error_message)
        mock_stdout.assert_not_called()
        mock_stderr.assert_any_call(StringContaining(error_message))

    @patch('sys.stderr.write')
    @patch('sys.stdout.write')
    def test_stdout(self, mock_stdout, mock_stderr):
        non_error_message = 'non-error message'
        Logger.init()
        Logger.info(non_error_message)
        mock_stderr.assert_not_called()
        mock_stdout.assert_any_call(StringContaining(non_error_message))


class TestLoggerCLI(TestDvc):
    def test(self):
        class A(object):
            quiet = True
            verbose = False

        args = A()
        CmdBase._set_loglevel(args)
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.CRITICAL)

        args.quiet = False
        args.verbose = True
        CmdBase._set_loglevel(args)
        self.assertEqual(Logger.logger().getEffectiveLevel(), logging.DEBUG)
