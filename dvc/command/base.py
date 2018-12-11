from dvc.logger import logger


class CmdBase(object):
    def __init__(self, args):
        from dvc.project import Project

        self.project = Project()
        self.config = self.project.config
        self.args = args
        self._set_loglevel(args)

    @staticmethod
    def _set_loglevel(args):
        if args.quiet:
            logger.be_quiet()

        elif args.verbose:
            logger.be_verbose()

    def run_cmd(self):
        from dvc.lock import LockError

        try:
            with self.project.lock:
                return self.run()
        except LockError as ex:
            logger.error('Failed to lock before running a command', ex)
            return 1

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        pass
