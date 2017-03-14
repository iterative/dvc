import os
import sys
import shutil
import fasteners

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
        self.set_skip_git_actions(parser)

        parser.add_argument('--random', help='not reproducible, output is random', action='store_true')
        parser.add_argument('--stdout', help='output std output to a file')
        parser.add_argument('--stderr', help='output std error to a file')
        pass

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.info('Cannot perform the command since NLX is busy and locked. Please retry the command later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            old_files = set(self.get_changed_files())

            GitWrapper.exec_cmd(self._args_unkn, self.args.stdout, self.args.stderr)

            new_files = set(self.get_changed_files())

            removed_files = old_files - new_files
            if removed_files != set():
                Logger.error('Error: existing files were removed in run command'.format(' '.join(removed_files)))

            created_files = new_files - old_files

            if created_files == set():
                Logger.error('Error: no files were changes in run command')
                return 1

            dobjs, errors = self.files_to_dobjs(created_files)

            if errors:
                for file in GitWrapper.abs_paths_to_relative(errors):
                    Logger.error('Error: file "{}" was created outside of the data directory'.format(file))

                for file in created_files:
                    rel_path = GitWrapper.abs_paths_to_relative([file])[0]
                    Logger.error('Removing created file: {}'.format(rel_path))
                    os.remove(file)

                Logger.error('Errors occurred. Please fix the errors and re-run the command.')
                return 1

            for dobj in dobjs:
                os.makedirs(os.path.dirname(dobj.cache_file_relative), exist_ok=True)
                shutil.move(dobj.data_file_relative, dobj.cache_file_relative)

                dobj.create_symlink()

                state_file = StateFile(dobj.state_file_relative, self.git)
                state_file.save()
                pass

            if self.skip_git_actions:
                self.not_committed_changes_warning()
                return 0

            message = 'NLX run: {}'.format(' '.join(sys.argv))
            self.git.commit_all_changes_and_log_status(message)
        finally:
            lock.release()

        return 0

    def files_to_dobjs(self, files):
        result = []
        errors = []

        for file in files:
            try:
                result.append(DataFileObj(file, self.git, self.config))
            except NotInDataDirError:
                errors.append(file)

        return result, errors

    def get_changed_files(self):
        statuses = GitWrapper.status_files()

        result = []
        for status, file in statuses:
            file_path = os.path.join(self.git.git_dir_abs, file)
            if os.path.isfile(file_path):
                result.append(file_path)
            else:
                files = []
                self.get_all_files_from_dir(file_path, files)
                for f in files:
                    result.append(f)

        return result

    def get_all_files_from_dir(self, dir, result):
        if not os.path.isdir(dir):
            raise RunError('Changed path {} is not directory'.format(dir))

        files = os.listdir(dir)
        for f in files:
            path = os.path.join(dir, f)
            if os.path.isfile(path):
                result.append(path)
            else:
                self.get_all_files_from_dir(path, result)
        pass

if __name__ == '__main__':
    import sys
    try:
        sys.exit(CmdRun().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
