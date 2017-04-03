import os
import sys
import shutil
import fasteners

from dvc.path.data_item import NotInDataDirError
from dvc.git_wrapper import GitWrapper
from dvc.command.base import CmdBase
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.repository_change import RepositoryChange
from dvc.state_file import StateFile
from dvc.utils import run
from dvc.utils import cached_property


class RunError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CmdRun(CmdBase):
    def __init__(self, parse_config=True, git_obj=None, config_obj=None):
        super(CmdRun, self).__init__(parse_config, git_obj, config_obj)
        pass

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        parser.add_argument('--random', help='not reproducible, output is random', action='store_true')
        parser.add_argument('--stdout', help='output std output to a file')
        parser.add_argument('--stderr', help='output std error to a file')
        parser.add_argument('--input', '-i', action='append',
                            help='Declare input data items for reproducible cmd')
        parser.add_argument('--output', '-o', action='append',
                            help='Declare output data items for reproducible cmd')
        parser.add_argument('--code', '-c', action='append',
                            help='Code dependencies which produce the output')
        pass

    @property
    def code_dependencies(self):
        return self.args.code or []

    @cached_property
    def declaration_input_data_items(self):
        return self._data_items_from_params(self.args.input, 'Input')

    @cached_property
    def declaration_output_data_items(self):
        return self._data_items_from_params(self.args.output, 'Output')

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.printing('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            self.run_command(self._args_unkn,
                             self._data_items_from_args(self._args_unkn),
                             self.args.stdout,
                             self.args.stderr)
            return self.commit_if_needed('DVC run: {}'.format(' '.join(sys.argv)))
        finally:
            lock.release()

        return 1

    def run_command(self, argv, data_items_from_args, stdout=None, stderr=None):
        repo_change = RepositoryChange(argv, stdout, stderr, self.git, self.config, self.path_factory)

        if not self.skip_git_actions and not self._validate_file_states(repo_change):
            self.remove_new_files(repo_change)
            raise RunError('Errors occurred.')

        output_set = set(self.declaration_output_data_items + repo_change.changed_data_items)
        output_files_dvc = [x.data.dvc for x in output_set]

        input_set = set(data_items_from_args + self.declaration_input_data_items)
        input_files_dvc = [x.data.dvc for x in input_set]

        code_dependencies_dvc = self.git.abs_paths_to_dvc(self.code_dependencies)

        result = []
        for data_item in repo_change.changed_data_items:
            Logger.debug('Move output file "{}" to cache dir "{}" and create a symlink'.format(
                data_item.data.relative, data_item.cache.relative))
            data_item.move_data_to_cache()

            Logger.debug('Create state file "{}"'.format(data_item.state.relative))
            state_file = StateFile(data_item.state.relative, self.git,
                                   input_files_dvc,
                                   output_files_dvc,
                                   code_dependencies_dvc,
                                   argv=argv)
            state_file.save()
            result.append(state_file)

        return result

    @staticmethod
    def remove_new_files(repo_change):
        for data_item in repo_change.new_data_items:
            Logger.error('Removing created file: {}'.format(data_item.data.relative))
            os.remove(data_item.data.relative)
        pass

    @staticmethod
    def _validate_file_states(repo_change):
        error = False
        for data_item in repo_change.removed_data_items:
            Logger.error('Error: file "{}" was removed'.format(data_item.data.relative))
            error = True

        for file in GitWrapper.abs_paths_to_relative(repo_change.externally_created_files):
            Logger.error('Error: file "{}" was created outside of the data directory'.format(file))
            error = True

        return not error

    def _data_items_from_args(self, argv):
        result = []

        for arg in argv:
            try:
                if os.path.isfile(arg):
                    data_item = self.path_factory.data_item(arg)
                    result.append(data_item)
            except NotInDataDirError:
                pass

        return result

    def _data_items_from_params(self, files, param_text):
        if not files:
            return []

        data_items, external = self.path_factory.to_data_items(files)
        if external:
            raise RunError('{} should point to data items from data dir: {}'.format(
                param_text, ', '.join(external))
            )
        return data_items


if __name__ == '__main__':
    run(CmdRun())
