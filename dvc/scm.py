import os
import git

from dvc.exceptions import DvcException
from dvc.command.common.branch_changer import BranchChanger


class FileNotInRepoError(DvcException):
    pass


class Base(object):
    @staticmethod
    def is_repo(root_dir):
        return True

    def ignore(self, path):
        pass

    def ignore_file(self):
        pass

    def ignore_list(self, p_list):
        return [self.ignore(path) for path in p_list]

    def add(self, paths):
        pass

    def commit(self, msg):
        pass

    def checkout(self, branch):
        pass

    def branch(self, branch):
        pass

    def brancher(self, branch, new_branch):
        return BranchChanger(self, branch, new_branch)

    def untracked_files(self):
        pass


class Git(Base):
    GITIGNORE = '.gitignore'
    GIT_DIR = '.git'

    def __init__(self, root_dir):
        self.root_dir = root_dir
        self.repo = git.Repo(root_dir)

    @staticmethod
    def is_repo(root_dir):
        git_dir = os.path.join(root_dir, Git.GIT_DIR)
        return os.path.isdir(git_dir)

    def ignore(self, path):
        entry = os.path.basename(path)
        gitignore = os.path.join(os.path.dirname(path), self.GITIGNORE)

        if not gitignore.startswith(self.root_dir):
            raise FileNotInRepoError()

        if os.path.exists(gitignore) and entry in open(gitignore, 'r').readline():
            return

        open(gitignore, 'a').write('\n' + entry)

    def ignore_file(self):
        return self.GITIGNORE

    def add(self, paths):
        self.repo.index.add(paths)

    def commit(self, msg):
        self.repo.index.commit(msg)

    def checkout(self, branch, create_new=False):
        if create_new:
            self.repo.git.checkout('HEAD', b=branch)
        else:
            self.repo.git.checkout(branch)

    def branch(self, branch):
        self.repo.git.branch(branch)

    def untracked_files(self):
        return self.repo.untracked_files


def SCM(root_dir):
    if Git.is_repo(root_dir):
        return Git(root_dir)

    return Base(root_dir)
