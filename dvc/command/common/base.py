import os

from dvc.project import Project
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.lock import LockError


class CmdBase(object):
    def __init__(self, args):
        self.project = Project(self._find_root())
        self.args = args
        self._set_loglevel(args)

    @staticmethod
    def _set_loglevel(args):
        if args.quiet and not args.verbose:
            Logger.be_quiet()
        elif not args.quiet and args.verbose:
            Logger.be_verbose()

    def _find_root(self):
        root = os.getcwd()
        while not os.path.ismount(root):
            dvc_dir = os.path.join(root, Project.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            root = os.path.dirname(root)
        msg = "Not a dvc repository (checked up to mount point {})"
        raise DvcException(msg.format(root))

    def run_cmd(self):
        try:
            with self.project.lock:
                return self.run()
        except LockError as ex:
            self.project.logger.error('Failed to lock before running a command', ex) 
            return 1

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        pass
