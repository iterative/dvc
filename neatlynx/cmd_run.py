import os
import shutil

from neatlynx.git_wrapper import GitWrapper
from neatlynx.cmd_base import CmdBase
from neatlynx.logger import Logger
from neatlynx.exceptions import NeatLynxException
from neatlynx.data_file_obj import DataFileObj, NotInDataDirError
from neatlynx.state_file import StateFile


class RunError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Run error: {}'.format(msg))


class CmdRun(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)
        pass

    def define_args(self, parser):
        parser.add_argument('--ignore-git-status', help='ignore git status', action='store_true')
        parser.add_argument('--random', help='not reproducible, output is random', action='store_true')
        parser.add_argument('--stdout', help='output std output to a file')
        parser.add_argument('--stderr', help='output std error to a file')
        pass

    def run(self):
        if not self.args.ignore_git_status and not self.git.is_ready_to_go():
            return 1

        GitWrapper.exec_cmd(self._args_unkn, self.args.stdout, self.args.stderr)

        statuses = GitWrapper.status_files()
        error = False
        dobjs = []
        for status, file in statuses:
            try:
                file_path = os.path.join(self.git.git_dir_abs, file)
                dobjs.append(DataFileObj(file_path, self.git, self.config))
            except NotInDataDirError:
                Logger.error('Error: file "{}" was created outside of the data directory'.format(file_path))
                error = True

        if error:
            Logger.error('Please fix the errors and re-run the command')
            return 1

        for dobj in dobjs:
            os.makedirs(os.path.dirname(dobj.cache_file_relative), exist_ok=True)
            shutil.move(dobj.data_file_relative, dobj.cache_file_relative)
            os.symlink(dobj.cache_file_relative, dobj.data_file_relative)

            state_file = StateFile(dobj.state_file_relative, self.git)
            state_file.save()
            pass

        return 0


if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdRun().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
