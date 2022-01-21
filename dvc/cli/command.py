import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class CmdBase(ABC):
    UNINITIALIZED = False

    def __init__(self, args):
        from dvc.repo import Repo

        os.chdir(args.cd)

        self.repo = Repo(uninitialized=self.UNINITIALIZED)
        self.config = self.repo.config
        self.args = args

    def do_run(self):
        with self.repo:
            return self.run()

    @abstractmethod
    def run(self):
        pass


class CmdBaseNoRepo(CmdBase):
    def __init__(self, args):  # pylint: disable=super-init-not-called
        self.args = args

        os.chdir(args.cd)

    def do_run(self):
        return self.run()
