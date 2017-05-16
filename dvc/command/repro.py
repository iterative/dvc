import os
import copy

from dvc.command.base import DvcLock
from dvc.command.import_file import CmdImportFile
from dvc.command.run import CmdRun
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.path.data_item import NotInDataDirError
from dvc.runtime import Runtime
from dvc.state_file import StateFile
from dvc.system import System


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
        self.set_no_git_actions(parser)

        parser.add_argument('target', metavar='', help='Data items to reproduce.', nargs='*')
        parser.add_argument('-f', '--force', action='store_true', default=False,
                            help='Reproduce even if dependencies were not changed.')
        parser.add_argument('-s', '--single-item', action='store_true', default=False,
                            help='Reproduce only single data item without recursive dependencies check.')
        pass

    @property
    def lock(self):
        return True

    @property
    def declaration_input_data_items(self):
        return []

    @property
    def declaration_output_data_items(self):
        return []

    def run(self):
        with DvcLock(self.is_locker, self.git):
            recursive = not self.parsed_args.single_item
            return self.repro_target(self.parsed_args.target, recursive, self.parsed_args.force)
        pass

    def repro_target(self, target, recursive, force):
        if not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        data_item_list, external_files_names = self.settings.path_factory.to_data_items(target)
        if external_files_names:
            Logger.error('Files from outside of the data directory "{}" could not be reproduced: {}'.
                         format(self.config.data_dir, ' '.join(external_files_names)))
            return 1

        if self.repro_data_items(data_item_list, recursive, force):
            return 0
        return 1

    def repro_data_items(self, data_item_list, recursive, force):
        error = False
        changed = False

        for data_item in data_item_list:
            try:
                target_commit = self.git.get_target_commit(data_item.data.relative)
                if target_commit is None:
                    msg = 'Data item "{}" cannot be reproduced: cannot obtain commit hashsum'
                    Logger.warn(msg.format(data_item.data.relative))
                    continue

                globally_changed_files = self.git.get_changed_files(target_commit)
                changed_files = set()
                change = ReproChange(data_item, self, globally_changed_files, recursive, force)
                if change.reproduce(changed_files):
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

        if error and not self.no_git_actions:
            Logger.error('Errors occurred. One or more repro cmd was not successful.')
            self.not_committed_changes_warning()

        return changed and not error


class ReproChange(object):
    def __init__(self, data_item, cmd_obj, globally_changed_files, recursive, force):
        self._data_item = data_item
        self.git = cmd_obj.git
        self._cmd_obj = cmd_obj
        self._globally_changed_files = globally_changed_files
        self._recursive = recursive
        self._force = force

        if not System.islink(data_item.data.relative):
            raise ReproError('data item {} is not symlink'.format(data_item.data.relative))

        try:
            self._state = StateFile.load(data_item.state.relative, self.git)
        except Exception as ex:
            raise ReproError('Error: state file "{}" cannot be loaded: {}'.
                             format(data_item.state.relative, ex))

        if not self.state.argv:
            raise ReproError('Error: parameter {} is not defined in state file "{}"'.
                             format(StateFile.PARAM_ARGV, data_item.state.relative))
        if len(self.state.argv) < 1:
            raise ReproError('Error: reproducible cmd in state file "{}" is too short'.
                             format(self.state.file))

        self._settings = copy.copy(self._cmd_obj.settings)
        self._settings.set_args(self.state.argv)
        pass

    def is_cache_exists(self):
        path = System.realpath(self._data_item.data.relative)
        return os.path.exists(path)

    @property
    def cmd_obj(self):
        return self._cmd_obj

    @property
    def state(self):
        return self._state

    def reproduce_data_item(self, changed_files):
        Logger.debug('Reproducing data item {}.'.format(self._data_item.data.dvc))

        for output_dvc in self._state.output_files:
            Logger.debug('Removing output file {} before reproduction.'.format(output_dvc))

            try:
                data_item = self.cmd_obj.settings.path_factory.existing_data_item_from_dvc_path(output_dvc)
                os.remove(data_item.data.relative)
            except Exception as ex:
                msg = 'Data item {} cannot be removed before reproduction: {}'
                Logger.error(msg.format(output_dvc, ex))

            changed_files.add(output_dvc)

        if self.state.is_import_file:
            Logger.debug('Reproducing data item {}. Re-import cmd: {}'.format(
                self._data_item.data.relative, ' '.join(self.state.argv)))

            if len(self.state.argv) != 2:
                msg = 'Data item "{}" cannot be re-imported because of arguments number {} is incorrect. Argv: {}'
                raise ReproError(msg.format(self._data_item.data.relative, len(self.state.argv), self.state.argv))

            input = self.state.argv[0]
            output = self.state.argv[1]

            cmd = CmdImportFile(self._settings)
            cmd.set_git_action(True)
            cmd.set_locker(False)

            Logger.info(u'Reproducing import command: {}'.format(output))
            if cmd.import_and_commit_if_needed(input, output, lock=True, check_if_ready=False) != 0:
                raise ReproError('Import command reproduction failed')
            return True
        elif self.state.is_run:
            cmd = CmdRun(self._settings)
            cmd.set_git_action(True)
            cmd.set_locker(False)

            Logger.info('Reproducing run command for data item {}. Args: {}'.format(
                self._data_item.data.relative, ' '.join(self.state.argv)))

            data_items_from_args, not_data_items_from_args = self.cmd_obj.argv_files_by_type(self.state.argv)
            if cmd.run_and_commit_if_needed(self.state.argv,
                                            data_items_from_args,
                                            not_data_items_from_args,
                                            self.state.stdout,
                                            self.state.stderr,
                                            self.state.shell,
                                            check_if_ready=False) != 0:
                raise ReproError('Run command reproduction failed')
            return True
        else:
            # Ignore EMPTY_FILE command
            pass
        pass

    def reproduce(self, changed_files):
        Logger.debug('Reproduce data item {}. recursive={}, force={}'.format(
            self._data_item.data.relative, self._recursive, self._force))

        if self.state.locked:
            Logger.debug('Data item {} is not reproducible'.format(self._data_item.data.relative))
            return False

        if self.is_repro_required(changed_files, self._data_item):
            if self._data_item.data.dvc not in changed_files:
                Logger.debug('Data item {} is going to be reproduced'.format(self._data_item.data.relative))
                self.reproduce_data_item(changed_files)
                changed_files.add(self._data_item.data.dvc)
                return True
            else:
                msg = 'Data item {} is not going to be reproduced because it is already reproduced'
                Logger.debug(msg.format(self._data_item.data.relative))
        else:
            Logger.debug('Data item {} is up to date'.format(self._data_item.data.relative))
            return False
        pass

    def were_dependencies_changed(self, changed_files, data_item_dvc):
        result = False

        for data_item in self.dependencies:
            change = ReproChange(data_item, self._cmd_obj, self._globally_changed_files, self._recursive, self._force)
            if change.reproduce(changed_files):
                result = True
                Logger.debug(u'Repro data item {}: dependency {} was changed'.format(
                    data_item_dvc, data_item.data.dvc))
            elif data_item.data.dvc in self._globally_changed_files:
                msg = u'Repro data item {}: dependency {} was not changed but the data item global checksum was changed'
                Logger.debug(msg.format(data_item_dvc, data_item.data.dvc))
                result = True
            else:
                msg = u'Repro data item {}: dependency {} was not changed'
                Logger.debug(msg.format(data_item_dvc, data_item.data.dvc))

        return result

    def is_repro_required(self, changed_files, data_item):
        state_file = StateFile.load(data_item.state.relative, self._settings)
        if state_file.locked:
            Logger.debug(u'Repro is not required for locked data item {}'.format(data_item.data.relative))
            return False

        is_dependency_check_required = self._recursive

        if not is_dependency_check_required and not self.is_cache_exists():
            is_dependency_check_required = True
            Logger.info(u'Reproduction {}. Force dependency check since cache file is missing.'.format(
                self._data_item.data.relative))

        if is_dependency_check_required:
            if self.were_dependencies_changed(changed_files, data_item.data.dvc):
                self.log_repro_reason(u'input dependencies were changed')
                return True

        if self._force:
            self.log_repro_reason(u'it was forced')
            return True

        if not self.is_cache_exists():
            self.log_repro_reason(u'cache file is missing.')
            return True

        if self.were_sources_changed(self._globally_changed_files):
            self.log_repro_reason(u'sources were changed')
            return True

        return False

    def log_repro_reason(self, reason):
        msg = u'Repro is required for data item {} because of {}'
        Logger.debug(msg.format(self._data_item.data.relative, reason))

    def were_sources_changed(self, globally_changed_files):
        were_sources_changed = self.git.were_files_changed(
            self.state.code_dependencies,
            self._settings.path_factory,
            globally_changed_files
        )
        return were_sources_changed

    @property
    def dependencies(self):
        dependency_data_items = []
        for input_file in self.state.input_files:
            try:
                data_item = self._settings.path_factory.data_item(input_file)
            except NotInDataDirError:
                raise ReproError(u'The dependency file "{}" is not a data item'.format(input_file))
            except Exception as ex:
                raise ReproError(u'Unable to reproduced the dependency file "{}": {}'.format(
                    input_file, ex))

            dependency_data_items.append(data_item)

        return dependency_data_items


if __name__ == '__main__':
    Runtime.run(CmdRepro)
