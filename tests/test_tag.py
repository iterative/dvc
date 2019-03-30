import os
import shutil
import filecmp

from dvc.main import main
from dvc.logger import logger

from tests.basic_env import TestDvc

from tests.utils import reset_logger_standard_output, reset_logger_error_output
from tests.utils.logger import MockLoggerHandlers, ConsoleFontColorsRemover


class TestTag(TestDvc):
    def test(self):
        fname = "file"
        shutil.copyfile(self.FOO, fname)

        stages = self.dvc.add(fname)
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertTrue(stage is not None)

        ret = main(["tag", "add", "v1", stage.path])
        self.assertEqual(ret, 0)

        os.unlink(fname)
        shutil.copyfile(self.BAR, fname)

        ret = main(["repro", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["tag", "add", "v2", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["tag", "list", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["checkout", stage.path + "@v1"])
        self.assertEqual(ret, 0)

        self.assertTrue(filecmp.cmp(fname, self.FOO, shallow=False))

        ret = main(["checkout", stage.path + "@v2"])
        self.assertEqual(ret, 0)

        self.assertTrue(filecmp.cmp(fname, self.BAR, shallow=False))


class TestTagAll(TestDvc):
    def test(self):
        ret = main(["add", self.FOO, self.BAR])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_standard_output()

            ret = main(["tag", "list"])
            self.assertEqual(ret, 0)

            self.assertEqual("", logger.handlers[0].stream.getvalue())

        ret = main(["tag", "add", "v1"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_standard_output()

            ret = main(["tag", "list"])
            self.assertEqual(ret, 0)

            self.assertEqual(
                logger.handlers[0].stream.getvalue(),
                "bar.dvc:\n"
                "  bar:\n"
                "    v1:\n"
                "      md5: 8978c98bb5a48c2fb5f2c4c905768afa\n"
                "foo.dvc:\n"
                "  foo:\n"
                "    v1:\n"
                "      md5: acbd18db4cc2f85cedef654fccc4a4d8\n"
                "\n",
            )

        ret = main(["tag", "remove", "v1"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_standard_output()

            ret = main(["tag", "list"])
            self.assertEqual(ret, 0)

            self.assertEqual("", logger.handlers[0].stream.getvalue())


class TestTagAddNoChecksumInfo(TestDvc):
    def test(self):
        ret = main(["run", "-o", self.FOO, "--no-exec"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_error_output()

            ret = main(["tag", "add", "v1", "foo.dvc"])
            self.assertEqual(ret, 0)

            self.assertEqual(
                "Warning: missing checksum info for 'foo'\n",
                logger.handlers[1].stream.getvalue(),
            )


class TestTagRemoveNoTag(TestDvc):
    def test(self):
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger), ConsoleFontColorsRemover():
            reset_logger_error_output()

            ret = main(["tag", "remove", "v1", "foo.dvc"])
            self.assertEqual(ret, 0)

            self.assertEqual(
                "Warning: tag 'v1' not found for 'foo'\n",
                logger.handlers[1].stream.getvalue(),
            )
