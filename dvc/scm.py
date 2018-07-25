import os

from dvc.exceptions import DvcException
from dvc.logger import Logger


class SCMError(DvcException):
    pass


class FileNotInRepoError(DvcException):
    pass


class Base(object):
    def __init__(self, root_dir=os.curdir):
        self.root_dir = os.path.abspath(os.path.realpath(root_dir))

    @staticmethod
    def is_repo(root_dir):
        return True

    def ignore(self, path):
        pass

    def ignore_remove(self, path):
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

    def brancher(self, branches=None, all_branches=False):
        if not branches and not all_branches:
            yield ''
            return

        saved = self.active_branch()
        if not branches:
            branches = self.list_branches() if all_branches else [saved]
        for branch in branches:
            print("OLOLO " + branch)
            self.checkout(branch)
            yield branch
        self.checkout(saved)

    def untracked_files(self):
        pass

    def is_tracked(self, path):
        pass

    def active_branch(self):
        pass

    def list_branches(self):
        pass

    def install(self):
        pass


class Git(Base):
    GITIGNORE = '.gitignore'
    GIT_DIR = '.git'

    def __init__(self, root_dir=os.curdir):
        super(Git, self).__init__(root_dir)

        import git
        from git.exc import InvalidGitRepositoryError
        try:
            self.repo = git.Repo(root_dir)
        except InvalidGitRepositoryError:
            raise SCMError('{} is not a git repository'.format(root_dir))

    @staticmethod
    def is_repo(root_dir):
        git_dir = os.path.join(root_dir, Git.GIT_DIR)
        return os.path.isdir(git_dir)

    def ignore_file(self):
        return self.GITIGNORE

    def _get_gitignore(self, path):
        entry = os.path.basename(path)
        gitignore = os.path.join(os.path.abspath(os.path.dirname(path)),
                                 self.GITIGNORE)
        gitignore = os.path.abspath(os.path.realpath(gitignore))

        if not gitignore.startswith(self.root_dir):
            raise FileNotInRepoError(path)

        return entry, gitignore

    def ignore(self, path):
        entry, gitignore = self._get_gitignore(path)

        ignore_list = []
        if os.path.exists(gitignore):
            ignore_list = open(gitignore, 'r').readlines()
            filtered = list(filter(lambda x: x.strip() == entry.strip(),
                                   ignore_list))
            if len(filtered) != 0:
                return

        content = entry
        if len(ignore_list) > 0:
            content = '\n' + content

        open(gitignore, 'a').write(content)

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore, 'r') as fd:
            lines = fd.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(gitignore, 'w') as fd:
            fd.writelines(filtered)

    def add(self, paths):
        # NOTE: GitPython is not currently able to handle index version >= 3.
        # See https://github.com/iterative/dvc/issues/610 for more details.
        try:
            self.repo.index.add(paths)
        except AssertionError as exc:
            msg = 'Failed to add \'{}\' to git. You can add those files '
            msg += 'manually using \'git add\'. '
            msg += 'See \'https://github.com/iterative/dvc/issues/610\' '
            msg += 'for more details.'
            Logger.error(msg.format(str(paths)), exc)

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
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        return len(self.repo.git.ls_files(path)) != 0

    def active_branch(self):
        return self.repo.active_branch.name

    def list_branches(self):
        return [h.name for h in self.repo.heads]

    def install(self):
        hook = os.path.join(self.root_dir,
                            self.GIT_DIR,
                            'hooks',
                            'post-checkout')
        if os.path.isfile(hook):
            msg = 'Git hook \'{}\' already exists.'
            raise SCMError(msg.format(os.path.relpath(hook)))
        with open(hook, 'w+') as fd:
            fd.write('#!/bin/sh\nexec dvc checkout\n')
        os.chmod(hook, 0o777)


def SCM(root_dir=os.curdir, no_scm=False):
    if Git.is_repo(root_dir):
        return Git(root_dir)

    return Base(root_dir)
