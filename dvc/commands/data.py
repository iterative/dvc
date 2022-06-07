import argparse
import logging
import os
from contextlib import contextmanager
from functools import partial

from funcy import log_durations

from dvc.cli.command import CmdBase
from dvc.cli.utils import fix_subparsers
from dvc.ui import ui

logger = logging.getLogger(__name__)

print_durations = partial(
    log_durations,
    ui.error_write
    if logger.isEnabledFor(logging.TRACE)  # type: ignore[attr-defined]
    else logger.trace,  # type: ignore[attr-defined]
)


class CmdDataStatus(CmdBase):
    @contextmanager
    def _patch_clone(self):
        from funcy import monkey

        from dvc import scm

        @monkey(scm, "clone")
        def clone(url, *args, **kwargs):
            with print_durations(f"cloning {os.path.basename(url)}"):
                return clone.original(url, *args, **kwargs)

        try:
            yield
        finally:
            scm.clone = clone.original

    @contextmanager
    def _patched_lock_repo(self):
        from dvc.lock import LockNoop
        from dvc.repo import lock_repo

        # rwlock is not atomic, so we may still want to have a repo lock here
        orig_lock = self.repo.lock
        try:
            self.repo.lock = LockNoop()
            with lock_repo(self.repo):
                yield
        finally:
            self.repo.lock = orig_lock

    @print_durations()
    def run(self):
        from dvc.repo.ls import _ls

        with print_durations("scm_status"):
            git_staged, git_unstaged, git_untracked = self.repo.scm.status()

        with self._patch_clone(), self._patched_lock_repo():
            with print_durations("ls"):
                # pylint: disable=protected-access
                ls_data = _ls(self.repo, "", recursive=True, dvc_only=True)

            with print_durations("status"):
                status_data = self.repo.status()

            with print_durations("diff"):
                diff_data = self.repo.diff()

        ui.write_json(
            {
                "diff": diff_data,
                "ls": ls_data,
                "status": status_data,
                "scm": {
                    "staged": git_staged,
                    "unstaged": git_unstaged,
                    "untracked": git_untracked,
                },
            }
        )
        return 0


def add_parser(subparsers, parent_parser):
    data_parser = subparsers.add_parser(
        "data",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    data_subparsers = data_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc data CMD --help` to display command-specific help.",
    )
    fix_subparsers(data_subparsers)
    data_status_parser = data_subparsers.add_parser(
        "status",
        parents=[parent_parser],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    data_status_parser.add_argument(
        "--json", action="store_true", default=False
    )
    data_status_parser.set_defaults(func=CmdDataStatus)
