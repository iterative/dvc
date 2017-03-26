import os

import fasteners

from dvc.command.base import CmdBase
from dvc.command.run import CmdRun
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.path.data_item import NotInDataDirError
from dvc.state_file import StateFile
from dvc.utils import run


class ReproError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdRun):
    def __init__(self):
        CmdBase.__init__(self)

        self._code =[]
        pass

    @property
    def code(self):
        return self._code

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        # self.add_string_arg(parser, 'target', 'Reproduce data file')
        parser.add_argument('target', metavar='', help='Data file to reproduce', nargs='*')
        pass

    # Overridden methods:

    @property
    def declaration_input_files(self):
        return []

    @property
    def declaration_output_files(self):
        return []

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            Logger.printing('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')
            return 1

        try:
            if not self.skip_git_actions and not self.git.is_ready_to_go():
                return 1

            data_item_list, external_files_names = self.path_factory.to_data_items(self.args.target)
            if external_files_names:
                Logger.error('Files from outside of the data directory "{}" could not be reproduced: {}'.
                             format(self.config.data_dir, ' '.join(external_files_names)))
                return 1

            error = False
            changed = False
            for data_item in data_item_list:
                try:
                    repro_change = ReproChange(data_item, self)
                    if repro_change.reproduce():
                        changed = True
                        Logger.info(u'Data file "{}" was reproduced.'.format(
                            data_item.data.relative
                        ))
                    else:
                        Logger.info(u'Reproduction is not required for data file "{}".'.format(
                            data_item.data.relative
                        ))
                except ReproError as err:
                    Logger.error('Error in reproducing data file {}: {}'.format(
                        data_item.data.relative, str(err)
                    ))
                    error = True
                    break

            if error and not self.skip_git_actions:
                Logger.error('Errors occurred. One or more repro cmd was not successful.')
                self.not_committed_changes_warning()
                return 1

            if changed:
                if self.skip_git_actions:
                    self.not_committed_changes_warning()
                    return 1

                message = 'DVC repro: {}'.format(' '.join(self.args.target))
                self.git.commit_all_changes_and_log_status(message)
        finally:
            lock.release()
        return 0


class ReproChange(object):
    def __init__(self, data_item, cmd_obj):
        self._data_item = data_item
        self.git = cmd_obj.git
        self._cmd_obj = cmd_obj
        self._state = StateFile.load(data_item.state.relative, self.git)

        cmd_obj._code = self._state.code_sources # HACK!!!

        argv = self._state.norm_argv

        if not argv:
            raise ReproError('Error: parameter {} is nor defined in state file "{}"'.
                             format(StateFile.PARAM_NORM_ARGV, data_item.state.relative))
        if len(argv) < 2:
            raise ReproError('Error: reproducible cmd in state file "{}" is too short'.
                             format(self._state.file))

        # if argv[0][-3:] != '.py':
        #     raise ReproError('Error: reproducible cmd format error in state file "{}"'.
        #                      format(self._state.file))

        self._repro_argv = argv
        pass

    def were_direct_dependencies_changed(self):
        return True

    def reproduce_data_file(self):
        Logger.debug('Reproducing data file "{}". Removing the file...'.format(
            self._data_item.data.relative))
        os.remove(self._data_item.data.relative)

        Logger.debug('Reproducing data file "{}". Re-runs cmd: {}'.format(
            self._data_item.data.relative, ' '.join(self._repro_argv)))
        return self._cmd_obj.run_command(self._repro_argv)

    def reproduce(self, force=False):
        were_input_files_changed = False

        if not self._state.is_reproducible:
            Logger.debug('Data file "{}" is not reproducible'.format(self._data_item.data.relative))
            return False

        for input_file in self._state.input_files:
            try:
                data_item = self._cmd_obj.path_factory.data_item(input_file)
            except NotInDataDirError:
                raise ReproError(u'The dependency files "{}" is not a data file'.format(input_file))
            except Exception as ex:
                raise ReproError(u'The dependency files "{}" can not be reproduced: {}'.format(
                                 input_file, ex))

            change = ReproChange(data_item, self._cmd_obj)
            if change.reproduce(force):
                were_input_files_changed = True

        was_source_code_changed = self.git.were_files_changed(self._data_item.data.relative,
                                                              self._state.code_sources)

        if not force and not was_source_code_changed and not were_input_files_changed:
            Logger.debug('Data file "{}" is up to date'.format(
                self._data_item.data.relative))
            return False

        return self.reproduce_data_file()

if __name__ == '__main__':
    run(CmdRepro())
