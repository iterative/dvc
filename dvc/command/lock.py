from __future__ import unicode_literals

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdLockBase(CmdBase):
    def run(self, unlock=False):
        for target in self.args.targets:
            try:
                self.repo.lock_stage(target, unlock=unlock)
            except DvcException:
                logger.error(
                    "failed to {}lock '{}'".format(
                        "un" if unlock else "", target
                    )
                )

                return 1
        return 0


class CmdLock(CmdLockBase):
    def run(self):
        return super(CmdLock, self).run(False)


class CmdUnlock(CmdLockBase):
    def run(self):
        return super(CmdUnlock, self).run(True)


def add_parser(subparsers, parent_parser):
    LOCK_HELP = "Lock DVC file.\ndocumentation: https://man.dvc.org/lock"
    lock_parser = subparsers.add_parser(
        "lock", parents=[parent_parser], description=LOCK_HELP, help=LOCK_HELP
    )
    lock_parser.add_argument("targets", nargs="+", help="DVC files.")
    lock_parser.set_defaults(func=CmdLock)

    UNLOCK_HELP = "Unlock DVC file.\ndocumentation: https://man.dvc.org/unlock"
    unlock_parser = subparsers.add_parser(
        "unlock",
        parents=[parent_parser],
        description=UNLOCK_HELP,
        help=UNLOCK_HELP,
    )
    unlock_parser.add_argument("targets", nargs="+", help="DVC files.")
    unlock_parser.set_defaults(func=CmdUnlock)
