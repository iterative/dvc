from dvc.command.common.common_error import CmdCommonError


class BranchChangerError(CmdCommonError):
    def __init__(self, branch, msg):
        super(CmdCommonError, self).__init__('Error in changing branch \'{}\': {}'.format(branch, msg))


class BranchChanger(object):
    def __init__(self, scm, branch, new_branch):
        if branch and new_branch:
            raise BranchChangerError("Commands conflict: --branch and --new-branch cannot be used at the same command")

        self.perform_action = branch or new_branch

        self.branch = branch if branch else new_branch
        self.create_new = new_branch is not None
        self.scm = scm

    def __enter__(self):
        if self.perform_action:
            code, _, err = self.scm.checkout(self.branch, self.create_new)
            if code != 0:
                raise BranchChangerError(self.branch, err)
        return self

    def __exit__(self, type, value, traceback):
        if self.perform_action:
            code, _, err = self.scm.checkout('-')
            if code != 0:
                raise BranchChangerError(self.branch, err)
