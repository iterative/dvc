from __future__ import unicode_literals

import colorama

import dvc.logger as logger


def fix_subparsers(subparsers):
    """Workaround for bug in Python 3. See more info at:
        https://bugs.python.org/issue16308
        https://github.com/iterative/dvc/issues/769

        Args:
            subparsers: subparsers to fix.
    """
    from dvc.utils.compat import is_py3

    if is_py3:  # pragma: no cover
        subparsers.required = True
        subparsers.dest = "cmd"


def append_doc_link(help_message, path):
    if not path:
        return help_message
    doc_base = "https://man.dvc.org/"
    return "{message}\ndocumentation: {blue}{base}{path}{nc}".format(
        message=help_message,
        base=doc_base,
        path=path,
        blue=colorama.Fore.CYAN,
        nc=colorama.Fore.RESET,
    )


class CmdBase(object):
    def __init__(self, args):
        from dvc.repo import Repo

        self.repo = Repo()
        self.config = self.repo.config
        self.args = args
        self.set_loglevel(args)

    @property
    def default_targets(self):
        """Default targets for `dvc repro` and `dvc pipeline`."""
        from dvc.stage import Stage

        msg = "assuming default target '{}'.".format(Stage.STAGE_FILE)
        logger.warning(msg)
        return [Stage.STAGE_FILE]

    @staticmethod
    def set_loglevel(args):
        """Sets log level from CLI arguments."""
        if args.quiet:
            logger.be_quiet()

        elif args.verbose:
            logger.be_verbose()

    def run_cmd(self):
        from dvc.lock import LockError

        try:
            with self.repo.lock:
                return self.run()
        except LockError:
            logger.error("failed to lock before running a command")
            return 1

    # Abstract methods that have to be implemented by any inheritance class
    def run(self):
        pass
