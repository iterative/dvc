import os

from dvc.project import Project


class CmdBase(object):
    def __init__(self, args):
        self.project = Project(self._find_root())
        self.args = args

        if args.quiet and not args.verbose:
            self.project.logger.be_quiet()
        elif not args.quiet and args.verbose:
            self.project.logger.be_verbose()

    def _find_root(self):
        root = os.getcwd()
        while not os.path.ismount(root):
            dvc_dir = os.path.join(root, Project.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            root = os.path.dirname(root)
        msg = "Not a dvc repository (or any parent up to mount point {})"
        Logger.error(msg.format(root))

    def run_cmd(self):
        with self.project.lock:
            with self.project.scm.brancher(self.args.branch, self.args.new_branch):
                return self.run()

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        pass
