import yaml
import os

from dvc.scm import Git
from dvc.utils.compat import StringIO
from mock import MagicMock
from contextlib import contextmanager


def spy(method_to_decorate):
    mock = MagicMock()

    def wrapper(self, *args, **kwargs):
        mock(*args, **kwargs)
        return method_to_decorate(self, *args, **kwargs)

    wrapper.mock = mock
    return wrapper


def get_gitignore_content():
    with open(Git.GITIGNORE, "r") as gitignore:
        return gitignore.read().splitlines()


def load_stage_file(path):
    with open(path, "r") as fobj:
        return yaml.safe_load(fobj)


def reset_logger_error_output():
    from dvc.logger import logger

    logger.handlers[1].stream = StringIO()


def reset_logger_standard_output():
    from dvc.logger import logger

    logger.handlers[0].stream = StringIO()


@contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)
