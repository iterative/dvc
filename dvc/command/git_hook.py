from dvc.command.base import CmdBaseNoRepo, fix_subparsers


class CmdHookBase(CmdBaseNoRepo):
    cmd = None
    collect_analytics = False

    def run(self):
        from dvc.exceptions import NotDvcRepoError
        from dvc.main import main
        from dvc.repo import Repo

        assert self.cmd
        try:
            repo = Repo()
            repo.close()
        except NotDvcRepoError:
            return 0

        return main([self.cmd], disable_analytics=True)


class CmdPreCommit(CmdHookBase):
    cmd = "status"


class CmdPostCheckout(CmdHookBase):
    cmd = "checkout"

    def run(self):
        # when we are running from pre-commit tool, it doesn't provide CLI
        # flags, but instead provides respective env vars that we could use.
        import os

        flag = os.environ.get("PRE_COMMIT_CHECKOUT_TYPE")
        if flag is None and len(self.args.args) >= 3:
            # see https://git-scm.com/docs/githooks#_post_checkout
            flag = self.args.args[2]

        # checking out some reference and not specific file.
        if flag != "1":
            return 0

        # make sure we are not in the middle of a rebase/merge, so we
        # don't accidentally break it with an unsuccessful checkout.
        # Note that git hooks are always running in repo root.
        if os.path.isdir(os.path.join(".git", "rebase-merge")):
            return 0

        return super().run()


class CmdPrePush(CmdHookBase):
    cmd = "push"


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
    pre_commit_parser.add_argument(
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool.",
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
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool.",
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
        "args", nargs="*", help="Arguments passed by GIT or pre-commit tool.",
    )
    pre_push_parser.set_defaults(func=CmdPrePush)
