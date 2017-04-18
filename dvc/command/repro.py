import os
import fasteners

from dvc.command.run import CmdRun
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.path.data_item import NotInDataDirError
from dvc.runtime import Runtime
from dvc.state_file import StateFile


class ReproError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Run error: {}'.format(msg))


class CmdRepro(CmdRun):
    def __init__(self, settings):
        super(CmdRun, self).__init__(settings)

        self._code = []
        pass

    @property
    def code_dependencies(self):
        return self._code

    def define_args(self, parser):
        self.set_skip_git_actions(parser)

        parser.add_argument('target', metavar='', help='Data item to reproduce', nargs='*')
        parser.add_argument('-f', '--force', action='store_true', default=False,
                            help='Force reproduction')
        pass

    @property
    def is_reproducible(self):
        return True

    @property
    def declaration_input_data_items(self):
        return []

    @property
    def declaration_output_data_items(self):
        return []

    def run(self):
        lock = fasteners.InterProcessLock(self.git.lock_file)
        gotten = lock.acquire(timeout=5)
        if not gotten:
            self.warning_dvc_is_busy()
            return 1

        try:
            return self.repro_target(self.parsed_args.target, self.parsed_args.force)
        finally:
            lock.release()

        pass

    def repro_target(self, target, force):
        if not self.skip_git_actions and not self.git.is_ready_to_go():
            return 1

        data_item_list, external_files_names = self.settings.path_factory.to_data_items(target)
        if external_files_names:
            Logger.error('Files from outside of the data directory "{}" could not be reproduced: {}'.
                         format(self.config.data_dir, ' '.join(external_files_names)))
            return 1

        if self.repro_data_items(data_item_list, force):
            return self.commit_if_needed('DVC repro: {}'.format(' '.join(target)))
        pass

    def repro_data_items(self, data_item_list, force):
        error = False
        changed = False

        for data_item in data_item_list:
            try:
                target_commit = self.git.get_target_commit(data_item.data.relative)
                repro_change = ReproChange(data_item, self, target_commit)
                if repro_change.reproduce(force):
                    changed = True
                    Logger.info(u'Data item "{}" was reproduced.'.format(
                        data_item.data.relative
                    ))
                else:
                    Logger.info(u'Reproduction is not required for data item "{}".'.format(
                        data_item.data.relative
                    ))
            except ReproError as err:
                Logger.error('Error in reproducing data item {}: {}'.format(
                    data_item.data.relative, str(err)
                ))
                error = True
                break

        if error and not self.skip_git_actions:
            Logger.error('Errors occurred. One or more repro cmd was not successful.')
            self.not_committed_changes_warning()

        return changed and not error


class ReproChange(object):
    def __init__(self, data_item, cmd_obj, target_commit):
        self._data_item = data_item
        self.git = cmd_obj.git
        self._cmd_obj = cmd_obj
        self._state = StateFile.load(data_item.state.relative, self.git)

        cmd_obj._code = self.state.code_dependencies

        self._target_commit = target_commit

        argv = self.state.norm_argv

        if not argv:
            raise ReproError('Error: parameter {} is nor defined in state file "{}"'.
                             format(StateFile.PARAM_NORM_ARGV, data_item.state.relative))
        if len(argv) < 2:
            raise ReproError('Error: reproducible cmd in state file "{}" is too short'.
                             format(self.state.file))

        self._repro_argv = argv
        pass

    @property
    def cmd_obj(self):
        return self._cmd_obj

    @property
    def state(self):
        return self._state

    def reproduce_data_file(self):
        Logger.debug('Reproducing data item "{}". Removing the file...'.format(
            self._data_item.data.dvc))
        os.remove(self._data_item.data.relative)

        Logger.debug('Reproducing data item "{}". Re-runs cmd: {}'.format(
            self._data_item.data.relative, ' '.join(self._repro_argv)))

        data_items_from_args = self.cmd_obj.data_items_from_args(self._repro_argv)
        return self.cmd_obj.run_command(self._repro_argv,
                                        data_items_from_args,
                                        self.state.stdout,
                                        self.state.stderr)

    def reproduce(self, force=False):
        Logger.debug('Reproduce data item {} with dependencies, force={}: {}'.format(
                     self._data_item.data.dvc,
                     force,
                     ', '.join([x.data.dvc for x in self.dependencies])))

        if not force and not self.state.is_reproducible:
            Logger.debug('Data item "{}" is not reproducible'.format(self._data_item.data.relative))
            return False

        were_input_files_changed = False
        for data_item in self.dependencies:
            change = ReproChange(data_item, self._cmd_obj, self._target_commit)
            if change.reproduce(force):
                were_input_files_changed = True

        was_source_code_changed = self.git.were_files_changed(self.state.code_dependencies + self.state.input_files,
                                                              self.cmd_obj.settings.path_factory,
                                                              self._target_commit)
        if was_source_code_changed:
            Logger.debug('Dependencies were changed for "{}"'.format(self._data_item.data.dvc))

        if not force and not was_source_code_changed and not were_input_files_changed:
            Logger.debug('Data item "{}" is up to date'.format(
                self._data_item.data.relative))
            return False

        return self.reproduce_data_file()

    @property
    def dependencies(self):
        dependency_data_items = []
        for input_file in self.state.input_files:
            try:
                data_item = self._cmd_obj.settings.path_factory.data_item(input_file)
            except NotInDataDirError:
                raise ReproError(u'The dependency file "{}" is not a data item'.format(input_file))
            except Exception as ex:
                raise ReproError(u'Unable to reproduced the dependency file "{}": {}'.format(
                    input_file, ex))

            dependency_data_items.append(data_item)

        return dependency_data_items


if __name__ == '__main__':
    Runtime.run(CmdRepro)
