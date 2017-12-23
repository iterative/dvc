from dvc.project import Project

class CmdBase(object):
    def __init__(self, args):
        self.project = Project('.')
        self.args = args

        if args.quiet and not args.verbose:
            self.project.logger.be_quiet()
        elif not args.quiet and args.verbose:
            self.project.logger.be_verbose()

    def run_cmd(self):
        with self.project.lock:
            with self.project.scm.brancher(self.args.branch, self.args.new_branch):
                return self.run()

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        pass
