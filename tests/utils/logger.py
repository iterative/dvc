import logging
from mock import patch
import dvc.logger as logger


class MockLoggerHandlers(object):
    def __init__(self, l, num=2):
        self._logger = l
        self._handlers = l.handlers
        self._num = num

    def __enter__(self):
        self._logger.handlers = [
            logging.FileHandler("tmp{}.log".format(i))
            for i in range(self._num)
        ]
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        for h in self._logger.handlers:
            h.close()
        self._logger.handlers = self._handlers


class ConsoleFontColorsRemover(object):
    def __init__(self):
        self.color_patch = patch.object(
            logger, "colorize", new=lambda x, color="": x
        )

    def __enter__(self):
        self.color_patch.start()

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.color_patch.stop()
