import os

from dvc.exceptions import DvcException


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
            filtered = list(filter(lambda x: x.strip() == entry.strip(), ignore_list))
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
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        return len(self.repo.git.ls_files(path)) != 0

    def active_branch(self):
        return self.repo.active_branch.name

    def list_branches(self):
        return [h.name for h in self.repo.heads]

    def install(self):
        hook = os.path.join(self.root_dir, self.GIT_DIR, 'hooks', 'post-checkout')
        if os.path.isfile(hook):
            raise SCMError('Git hook \'{}\' already exists.'.format(os.path.relpath(hook)))
        with open(hook, 'w+') as fd:
            fd.write('#!/bin/sh\nexec dvc checkout\n')
        os.chmod(hook, 0o777)


def SCM(root_dir=os.curdir, no_scm=False):
    if Git.is_repo(root_dir):
        return Git(root_dir)

    return Base(root_dir)
