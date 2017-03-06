import os
import subprocess

from neatlynx.exceptions import NeatLynxException


class GitCmdError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, msg)


class GitWrapperI(object):
    def __init__(self, git_dir=None, commit=None):
        self._git_dir = git_dir
        self._commit = commit

    @property
    def git_dir(self):
        return self._git_dir

    @property
    def git_dir_abs(self):
        return os.path.realpath(self.git_dir)

    @property
    def curr_commit(self):
        return self._commit


class GitWrapper(GitWrapperI):
    def __init__(self):
        GitWrapperI.__init__(self)

    @staticmethod
    def _exec_cmd(cmd):
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        out, err = map(lambda s: s.decode().strip('\n\r'), p.communicate())

        return p.returncode, out, err

    @property
    def git_dir(self):
        if self._git_dir:
            return self._git_dir

        try:
            code, out, err = GitWrapper._exec_cmd(['git', 'rev-parse', '--show-toplevel'])

            if code != 0:
                raise GitCmdError('Git command error - {}'.format(err))

            self._git_dir = out
            return self._git_dir
        except GitCmdError as e:
            raise
        except Exception as e:
            raise GitCmdError('Unable to run git command: {}'.format(e))
        pass

    @staticmethod
    def status_files():
        code, out, err = GitWrapper._exec_cmd(['git', 'status' '--porcelain'])
        if code != 0:
            raise GitCmdError('Git command error - {}'.format(err))

        result = []
        if len(err) > 0:
            lines = out.split('\n')
            for line in lines:
                status, file = line.s.strip().split(' ', 1)
                result.append((status, file ))

        return result

    @property
    def curr_commit(self):
        if self._commit is None:
            code, out, err = GitWrapper._exec_cmd(['git', 'rev-parse' 'HEAD'])
            if code != 0:
                raise GitCmdError('Git command error - {}'.format(err))
            self._commit = out
        return self._commit
