from __future__ import unicode_literals

import argparse
import logging

from dvc.command.base import append_doc_link
from dvc.command.base import CmdBase
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdLockBase(CmdBase):
    def _run(self, unlock):
        for target in self.args.targets:
            try:
                self.repo.lock_stage(target, unlock=unlock)
            except DvcException:
                logger.exception(
                    "failed to {}lock '{}'".format(
                        "un" if unlock else "", target
                    )
                )

                return 1
        return 0


class CmdLock(CmdLockBase):
    def run(self):
        return self._run(False)


class CmdUnlock(CmdLockBase):
    def run(self):
        return self._run(True)


def add_parser(subparsers, parent_parser):
    LOCK_HELP = "Lock DVC-files."
    lock_parser = subparsers.add_parser(
        "lock",
        parents=[parent_parser],
        description=append_doc_link(LOCK_HELP, "lock"),
        help=LOCK_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    lock_parser.add_argument("targets", nargs="+", help="DVC-files to lock.")
    lock_parser.set_defaults(func=CmdLock)

    UNLOCK_HELP = "Unlock DVC-files."
    unlock_parser = subparsers.add_parser(
        "unlock",
        parents=[parent_parser],
        description=append_doc_link(UNLOCK_HELP, "unlock"),
        help=UNLOCK_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    unlock_parser.add_argument(
        "targets", nargs="+", help="DVC-files to unlock."
    )
    unlock_parser.set_defaults(func=CmdUnlock)
