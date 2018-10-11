import os
import shutil

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import fix_env


class SCMError(DvcException):
    pass


class FileNotInRepoError(DvcException):
    pass


class Base(object):
    def __init__(self, root_dir=os.curdir, project=None):
        self.project = project
        self.root_dir = root_dir

    @staticmethod
    def is_repo(root_dir):
        return True

    @staticmethod
    def is_submodule(root_dir):
        return True
    
    @staticmethod
    def get_add_reminder(files_to_add):
        return ''

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

    def brancher(self,
                 branches=None,
                 all_branches=False,
                 tags=None,
                 all_tags=False):
        if not branches and not all_branches \
           and not tags and not all_tags:
            yield ''
            return

        saved = self.active_branch()
        revs = []

        if branches is not None:
            revs.extend(branches)
        elif all_branches:
            revs.extend(self.list_branches())
        elif tags is not None:
            revs.extend(tags)
        elif all_tags:
            revs.extend(self.list_tags())
        else:
            revs.extend([saved])

        for rev in revs:
            self.checkout(rev)
            yield rev

        self.checkout(saved)

    def untracked_files(self):
        pass

    def is_tracked(self, path):
        pass

    def active_branch(self):
        pass

    def list_branches(self):
        pass

    def list_tags(self):
        pass

    def install(self):
        pass


class Git(Base):
    GITIGNORE = '.gitignore'
    GIT_DIR = '.git'

    def __init__(self, root_dir=os.curdir, project=None):
        super(Git, self).__init__(root_dir, project=project)

        import git
        from git.exc import InvalidGitRepositoryError
        try:
            self.repo = git.Repo(root_dir)
        except InvalidGitRepositoryError:
            msg = '{} is not a git repository'
            raise SCMError(msg.format(root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        lp = env.get('LD_LIBRARY_PATH', None)
        self.repo.git.update_environment(LD_LIBRARY_PATH=lp)

    @staticmethod
    def is_repo(root_dir):
        return os.path.isdir(Git._get_git_dir(root_dir))

    @staticmethod
    def is_submodule(root_dir):
        return os.path.isfile(Git._get_git_dir(root_dir))
    
    @staticmethod
    def get_add_reminder(files_to_add):
        msg = '\nTo track the changes with git run:\n\n'
        msg += '\tgit add ' + " ".join(files_to_add)

    @staticmethod
    def _get_git_dir(root_dir):
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def dir(self):
        return self.repo.git_dir

    def ignore_file(self):
        return os.path.join(self.root_dir, '.dvc', self.GITIGNORE)

    def _get_gitignore(self, path):
        assert os.path.isabs(path)
        entry = os.path.basename(path)
        gitignore = os.path.join(os.path.dirname(path), self.GITIGNORE)

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

        msg = "Adding '{}' to '{}'.".format(os.path.relpath(path),
                                            os.path.relpath(gitignore))
        Logger.info(msg)

        content = entry
        if len(ignore_list) > 0:
            content = '\n' + content

        with open(gitignore, 'a') as fd:
            fd.write(content)

        if self.project is not None:
            self.project._files_to_scm_add.append(os.path.relpath(gitignore))

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore, 'r') as fd:
            lines = fd.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(gitignore, 'w') as fd:
            fd.writelines(filtered)

        if self.project is not None:
            self.project._files_to_scm_add.append(os.path.relpath(gitignore))

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
        msg = 'Changes to the following files were added to git:\n' + \
              ''.join('\t{}\n'.format(os.path.relpath(p)) for p in paths) + \
              '\nYou can now commit the changes.'
        Logger.info(msg)

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

    def list_tags(self):
        return [t.name for t in self.repo.tags]

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

class Mercurial(Base):
    HGIGNORE = '.hgignore'
    HG_DIR = '.hg'

    def __init__(self, root_dir=os.curdir, project=None):
        super(Mercurial, self).__init__(root_dir, project=project)

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        lp = env.get('LD_LIBRARY_PATH', None)

        import hglib
        from hglib.error import ServerError
        from hglib.client import hgclient
        try:
            # Mercurial python interface gives no nice way to set the
            # environment variables the command server will be spawned with,
            # so we have to reach in and change its private _env attribute.
            # Thus we need to initialize the client without connecting, fix
            # the environment, and only then open
            self.client = hgclient(root_dir, None, None, connect=False)
            self.client._env.update(env)
            self.client.open()
        except ServerError:
            # Note that if we get here, this path had a .hg directory, but
            # the client did not successfully connect to the Mercurial
            # command server
            msg = '{} looks like a Mercurial repository, but dvc could not ' \
                  'initialize a Mercurial command server there.'
            raise SCMError(msg.format(root_dir))



    @staticmethod
    def is_repo(root_dir):
        return os.path.isdir(Mercurial._get_hg_dir(root_dir))

    @staticmethod
    def is_submodule(root_dir):
        # Submodules not supported in Mercurial
        return False

    @staticmethod
    def get_add_reminder(files_to_add):
        msg = '\nTo track the changes with Mercurial run:\n\n'
        msg += '\thg add ' + " ".join(files_to_add)

    @staticmethod
    def _get_hg_dir(root_dir):
        return os.path.join(root_dir, Mercurial.HG_DIR)

    @property
    def dir(self):
        return self.client.root()

    def ignore_file(self):
        return os.path.join(self.root_dir, self.HGIGNORE)

    def _get_hgignore(self, path):
        assert os.path.isabs(path)
        if not path.startswith(self.root_dir):
            raise FileNotInRepoError(path)
        entry = os.path.relpath(path, self.root_dir)
        hgignore = os.path.join(self.root_dir, self.HGIGNORE)

        return entry, hgignore

    def ignore(self, path):
        entry, hgignore = self._get_hgignore(path)

        ignore_list = []
        if os.path.exists(hgignore):
            ignore_list = open(hgignore, 'r').readlines()
            filtered = list(filter(lambda x: x.strip() == entry.strip(),
                                   ignore_list))
            if len(filtered) != 0:
                return

        msg = "Adding '{}' to '{}'.".format(os.path.relpath(path),
                                            os.path.relpath(hgignore))
        Logger.info(msg)

        content = entry
        if len(ignore_list) > 0:
            content = '\n' + content

        with open(hgignore, 'a+') as fd:
            fd.write(content)

        if self.project is not None:
            self.project._files_to_scm_add.append(os.path.relpath(hgignore))

    def ignore_remove(self, path):
        entry, hgignore = self._get_hgignore(path)

        if not os.path.exists(hgignore):
            return

        with open(hgignore, 'r') as fd:
            lines = fd.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(hgignore, 'w') as fd:
            fd.writelines(filtered)

        if self.project is not None:
            self.project._files_to_scm_add.append(os.path.relpath(hgignore))

    def add(self, paths):
        try:
            self.client.add(paths)
        except AssertionError as exc:
            msg = 'Failed to add \'{}\' to hg. You can add those files '
            msg += 'manually using \'hg add\'. '
            msg += 'See \'https://github.com/iterative/dvc/issues/610\' '
            msg += 'for more details.'
            Logger.error(msg.format(str(paths)), exc)
        msg = 'Changes to the following files were added to mercurial:\n' + \
              ''.join('\t{}\n'.format(os.path.relpath(p)) for p in paths) + \
              '\nYou can now commit these changesets.'
        Logger.info(msg)

    def commit(self, msg):
        self.client.commit(msg)

    def checkout(self, branch, create_new=False):
        if create_new:
            self.client.branch(name=branch)
        else:
            self.client.checkout(rev=branch, check=True)

    def branch(self, branch):
        self.client.branch(name=branch)

    def untracked_files(self):
        files = self.client.status(unknown=True)
        return [os.path.join(self.dir, fname) for (_, fname) in files]

    def is_tracked(self, path):
        return path in [fpath for (_, _, _, _, fpath) in self.client.manifest()]

    def active_branch(self):
        return self.client.branch()

    def list_branches(self):
        return [name for (name, _, _) in self.client.branches()]

    def list_tags(self):
        return [name for (name, _, _, _) in self.client.tags()]

    def install(self):
        hgrc = os.path.join(self.root_dir,
                            self.HG_DIR,
                            'hgrc')
        hgrc_backup = hgrc + '.dvc_backup'
        if os.path.isfile(hgrc_backup):
            msg = 'Found an existing dvc backup for the local mercurial ' \
                  'configuration file, called hgrc.dvc_backup'
            raise SCMError(msg) 
        if not os.path.isfile(hgrc):
            msg = 'Local mercurial hgrc file not found. (Looked here:)\n' \
                  '  \'{}\''
            raise SCMError(msg.format(os.path.relpath(hgrc)))

        shutil.copy(hgrc, hgrc + '.dvc_backup')
        with open(hgrc, 'a+') as fd:
            fd.write('[hooks]\nupdate = dvc checkout\n')


def SCM(root_dir, no_scm=False, project=None):
    if Git.is_repo(root_dir) or Git.is_submodule(root_dir):
        return Git(root_dir, project=project)
    if Mercurial.is_repo(root_dir) or Mercurial.is_submodule(root_dir):
        return Mercurial(root_dir, project=project)
    return Base(root_dir, project=project)
