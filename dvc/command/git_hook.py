import logging
import os

from dvc.utils import format_link
from dvc.command.base import CmdBaseNoRepo, fix_subparsers
from dvc.exceptions import NotDvcRepoError

logger = logging.getLogger(__name__)


class CmdHookBase(CmdBaseNoRepo):
    def run(self):
        from dvc.repo import Repo

        try:
            repo = Repo()
            repo.close()
        except NotDvcRepoError:
            return 0

        return self._run()


class CmdPreCommit(CmdHookBase):
    def _run(self):
        from dvc.main import main

        return main(["status"])


class CmdPostCheckout(CmdHookBase):
    def run(self):
        # checking out some reference and not specific file.
        if self.args.flag != "1":
            return 0

        # make sure we are not in the middle of a rebase/merge, so we
        # don't accidentally break it with an unsuccessful checkout.
        # Note that git hooks are always running in repo root.
        if os.path.isdir(os.path.join(".git", "rebase-merge")):
            return 0

        from dvc.main import main

        return main(["checkout"])


class CmdPrePush(CmdHookBase):
    def run(self):
        from dvc.main import main

        return main(["push"])


def add_parser(subparsers, parent_parser):
    GIT_HOOK_HELP = "Run GIT hook."

    git_hook_parser = subparsers.add_parser(
        "git-hook",
        parents=[parent_parser],
        description=GIT_HOOK_HELP,
        add_help=False,
    )

    git_hook_subparsers = git_hook_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc daemon CMD --help` for command-specific help.",
    )

    fix_subparsers(git_hook_subparsers)

    PRE_COMMIT_HELP = "Run pre-commit GIT hook."
    pre_commit_parser = git_hook_subparsers.add_parser(
        "pre-commit",
        parents=[parent_parser],
        description=PRE_COMMIT_HELP,
        help=PRE_COMMIT_HELP,
    )
    pre_commit_parser.set_defaults(func=CmdPreCommit)

    POST_CHECKOUT_HELP = "Run post-checkout GIT hook."
    post_checkout_parser = git_hook_subparsers.add_parser(
        "post-checkout",
        parents=[parent_parser],
        description=POST_CHECKOUT_HELP,
        help=POST_CHECKOUT_HELP,
    )
    post_checkout_parser.add_argument(
        "old_ref",
        help="Old ref provided by GIT (see {})".format(
            format_link("https://git-scm.com/docs/githooks#_post_checkout")
        ),
    )
    post_checkout_parser.add_argument(
        "new_ref",
        help="New ref provided by GIT (see {})".format(
            format_link("https://git-scm.com/docs/githooks#_post_checkout")
        ),
    )
    post_checkout_parser.add_argument(
        "flag",
        help="Flag provided by GIT (see {})".format(
            format_link("https://git-scm.com/docs/githooks#_post_checkout")
        ),
    )
    post_checkout_parser.set_defaults(func=CmdPostCheckout)

    PRE_PUSH_HELP = "Run pre-push GIT hook."
    pre_push_parser = git_hook_subparsers.add_parser(
        "pre-push",
        parents=[parent_parser],
        description=PRE_PUSH_HELP,
        help=PRE_PUSH_HELP,
    )
    pre_push_parser.add_argument(
        "name",
        help="Name provided by GIT (see {})".format(
            format_link("https://git-scm.com/docs/githooks#_pre_push")
        ),
    )
    pre_push_parser.add_argument(
        "location",
        help="Location provided by GIT (see {})".format(
            format_link("https://git-scm.com/docs/githooks#_pre_push")
        ),
    )
    pre_push_parser.set_defaults(func=CmdPrePush)
