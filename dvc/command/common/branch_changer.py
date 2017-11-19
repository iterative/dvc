from dvc.command.common.common_error import CmdCommonError


class BranchChangerError(CmdCommonError):
    def __init__(self, msg):
        super(CmdCommonError, self).__init__('DVC locker error: {}'.format(msg))


class BranchChanger(object):
    def __init__(self, branch, new_branch, git):
        if branch and new_branch:
            raise BranchChangerError("Commands conflict: --branch and --new-branch cannot be used at the same command")

        self.perform_action = branch or new_branch

        self.branch = branch if branch else new_branch
        self.create_new = new_branch is not None
        self.git = git

    def __enter__(self):
        if self.perform_action:
            print('========PERFORM')
            self.git.checkout(self.branch, self.create_new)
        return self

    def __exit__(self, type, value, traceback):
        if self.perform_action:
            self.git.checkout_previous()
