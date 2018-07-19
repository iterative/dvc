import colorama
import logging

from dvc.command.base import CmdBase
from dvc.config import Config
from dvc.logger import Logger

from tests.basic_env import TestDvc


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
